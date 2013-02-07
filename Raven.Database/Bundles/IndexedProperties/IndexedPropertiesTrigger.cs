using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Util;
using Raven.Json.Linq;
using Document = Lucene.Net.Documents.Document;
using Raven.Abstractions.Extensions;
using Raven.Database.Json;
using System.Diagnostics;
using System.Collections.Concurrent;

namespace Raven.Bundles.IndexedProperties
{
	[InheritedExport(typeof(AbstractIndexUpdateTrigger))]
	[ExportMetadata("Bundle", "IndexedProperties")]
	public class IndexedPropertiesTrigger : AbstractIndexUpdateTrigger
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
		{
			//Only apply the trigger if there is a setup doc for this particular index
            var jsonSetupDoc = Database.Get(IndexedPropertiesSetupDoc.IdPrefix + indexName, null);
			if (jsonSetupDoc == null)
				return null;
			var abstractViewGenerator = Database.IndexDefinitionStorage.GetViewGenerator(indexName);
			var setupDoc = jsonSetupDoc.DataAsJson.JsonDeserialization<IndexedPropertiesSetupDoc>();
			return new IndexPropertyBatcher(Database, setupDoc, indexName, abstractViewGenerator);
		}

		public class IndexPropertyBatcher : AbstractIndexUpdateTriggerBatcher
		{
			private readonly DocumentDatabase database;
			private readonly IndexedPropertiesSetupDoc setupDoc;
			private readonly string index;
			private readonly AbstractViewGenerator viewGenerator;

            // TODO combine the concurrentSet and concurrentDictionary into 1, maybe don't need both?
            private readonly ConcurrentSet<string> itemsToRemove =
                new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
            private readonly ConcurrentDictionary<string, Dictionary<string, object>> cachedValues = 
                new ConcurrentDictionary<string, Dictionary<string, object>>();

			public IndexPropertyBatcher(DocumentDatabase database, IndexedPropertiesSetupDoc setupDoc, string index, AbstractViewGenerator viewGenerator)
			{
				this.database = database;
				this.setupDoc = setupDoc;
				this.index = index;
				this.viewGenerator = viewGenerator;
			}

			public override void OnIndexEntryDeleted(string entryKey)
			{
				//Want to handle this scenario:
				// - Customer/1 has 2 orders (order/3 & order/5)
				// - Map/Reduce runs and AvgOrderCost in "customer/1" is set to the average cost of "order/3" and "order/5" (8.56 for example)
				// - "order/3" and "order/5" are deleted (so customer/1 will no longer be included in the results of the Map/Reduce
				// - I think we need to write back to the "customer/1" doc and delete the AvgOrderCost field in the Json,
                //   otherwise it'll still have the last value of 8.56

                if (String.IsNullOrEmpty(entryKey))
                {
                    log.Warn("Null or empty \"entryKey\" provided, '{0}' for index '{1}'", setupDoc.DocumentKey, index);
                    return;
                }

                Console.WriteLine("ItemsToRemove - Add - {0}", entryKey);
				itemsToRemove.Add(entryKey);
			}

			public override void OnIndexEntryCreated(string entryKey, Document document)
			{
				var resultDocId = document.GetField(setupDoc.DocumentKey);
				if (resultDocId == null)
				{
					log.Warn("Could not find document id property '{0}' in '{1}' for index '{2}'", setupDoc.DocumentKey, entryKey, index);
					return;
				}

				var documentId = resultDocId.StringValue;

                Console.WriteLine("ItemsToRemove - Remove - {0}", documentId);
				itemsToRemove.TryRemove(documentId);

				var resultDoc = database.Get(documentId, null);
				if (resultDoc == null)
				{
					log.Warn("Could not find a document with the id '{0}' for index '{1}'", documentId, index);
					return;
				}

				var entityName = resultDoc.Metadata.Value<string>(Constants.RavenEntityName);
				if (entityName != null && viewGenerator.ForEntityNames.Contains(entityName))
				{
					log.Warn(
						"Rejected update for a potentially recursive update on document '{0}' because the index '{1}' includes documents with entity name of '{2}'",
						documentId, index, entityName);
					return;
				}
				if (viewGenerator.ForEntityNames.Count == 0)
				{
					log.Warn(
						"Rejected update for a potentially recursive update on document '{0}' because the index '{1}' includes all documents",
						documentId, index);
					return;
				}

				var changesMade = false;
                if (setupDoc.Type == IndexedPropertiesType.FieldMapping)                
                    changesMade = ApplySimpleFieldMappings(document, resultDoc);                
                else if (setupDoc.Type == IndexedPropertiesType.Scripted)                
                    changesMade = ApplyScript(document, ref resultDoc);
                
                if (changesMade)                
                    database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
			}

            private bool ApplyScript(Document document, ref JsonDocument resultDoc)
            {
                var changesMade = false;
                var scriptedJsonPatcher = new ScriptedJsonPatcher(database);
                var values = new Dictionary<string, object>();
                // Make all the fields inside the Lucene doc, available as variables inside the script
                foreach (var field in document.GetFields()
                    .Where(f => f.Name.EndsWith("_Range") == false && f.Name != Constants.ReduceKeyFieldName))
                {
                    // It seems like all the fields from the Map/Reduce results come out as strings
                    // Try and confirm this, if so we don't need to worry about NumericField, StringField etc
                    values.Add(field.Name, field.StringValue);
                }
                var patchRequest = new ScriptedPatchRequest { Script = setupDoc.Script, Values = values };
                try
                {                    
                    var timer = Stopwatch.StartNew();
                    var result = scriptedJsonPatcher.Apply(resultDoc.DataAsJson, patchRequest);                    
                    resultDoc.DataAsJson = RavenJObject.FromObject(result);
                    timer.Stop();
                    var msecs = timer.Elapsed.TotalMilliseconds;
                    changesMade = true;
                    Console.WriteLine("CachedValues - Add - {0} - {1}", resultDoc.Key, String.Join(", ", values));
                    cachedValues.AddOrUpdate(resultDoc.Key, values, (str, dict) => values);
                }
                catch (Exception e)
                {
                    log.WarnException("Could not process Indexed Properties script for " + resultDoc.Key + ", skipping this document", e);
                }
                return changesMade;
            }

            private bool ApplySimpleFieldMappings(Document document, JsonDocument resultDoc)
            {
                bool changesMade = false;
                foreach (var mapping in setupDoc.FieldNameMappings)
                {
                    var field =
                        document.GetFieldable(mapping.Key + "_Range") ??
                        document.GetFieldable(mapping.Key);
                    if (field == null)
                        continue;
                    var numericField = field as NumericField;
                    if (numericField != null)
                    {
                        resultDoc.DataAsJson[mapping.Value] = new RavenJValue(numericField.NumericValue);
                    }
                    else
                    {
                        resultDoc.DataAsJson[mapping.Value] = field.StringValue;
                    }
                    changesMade = true;
                }
                return changesMade;
            }

			public override void Dispose()
			{
                Console.WriteLine("Dispose - ItemsToRemove - {0}", String.Join(", ", itemsToRemove));
                Console.WriteLine("Dispose - CachedValues - {0}", String.Join(", ", cachedValues.Keys));
				foreach (var documentId in itemsToRemove)
				{
					var resultDoc = database.Get(documentId, null);
					if (resultDoc == null)
					{
						log.Warn("Could not find a document with the id '{0}' for index '{1}", documentId, index);
						return;
					}
					var changesMade = false;
                    if (setupDoc.Type == IndexedPropertiesType.FieldMapping)
                    {
                        foreach (var mapping in from mapping in setupDoc.FieldNameMappings
                                                where resultDoc.DataAsJson.ContainsKey(mapping.Value)
                                                select mapping)
                        {
                            resultDoc.DataAsJson.Remove(mapping.Value);
                            changesMade = true;
                        }
                    }
                    else if (setupDoc.Type == IndexedPropertiesType.Scripted)
                    {
                        // How do we work out which fields to delete??? We need to know the fields that the script originally wrote???
                        // With the mappings it easier, but with the script we need to capture what was done when it was run to be sure?
                        // Do we need to match the FieldMapping behaviour, i.e. removing the fields from the Json that
                        // we previously mapped when the index item was created??? Maybe we just have to remove all possible fields?
                        var values = new Dictionary<string, object>();
                        if (cachedValues.TryGetValue(documentId, out values) == false)
                            continue;

                        foreach (var field in values)                
                        {
                            if (resultDoc.DataAsJson.ContainsKey(field.Key))
                                resultDoc.DataAsJson.Remove(field.Key);
                            changesMade = true;
                        }
                    }

					if (changesMade)
						database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);		
				}

				base.Dispose();
			}
		}
	}
}

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
			private readonly ConcurrentSet<string> itemsToRemove = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

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
				// - I think we need to write back to the "customer/1" doc and delete the AvgOrderCost field in the Json (otherwise it'll still have the last value of 8.56)

				RavenJObject entry;
				try
				{
					entry = RavenJObject.Parse(entryKey);
				}
				catch (Exception e)
				{
					log.WarnException("Could not properly parse entry key for index: " + index,e);
					return;

				}
				var documentId = entry.Value<string>(setupDoc.DocumentKey);
				if(documentId == null)
				{
					log.Warn("Could not find document id property '{0}' in '{1}' for index '{2}'", setupDoc.DocumentKey, entryKey, index);
					return;
				}

				itemsToRemove.Add(documentId);
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
                {
                    changesMade = ApplySimpleFieldMappings(document, resultDoc, changesMade);
                }
                else if (setupDoc.Type == IndexedPropertiesType.Scripted)
                {
                    changesMade = ApplyScript(document, ref resultDoc, changesMade);
                }
                if (changesMade)                
                    database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
			}

            private bool ApplyScript(Document document, ref JsonDocument resultDoc, bool changesMade)
            {
                var scriptedJsonPatcher = new ScriptedJsonPatcher(database);
                var values = new Dictionary<string, object>();
                foreach (var field in document.GetFields()
                    .Where(f => f.Name.EndsWith("_Range") == false && f.Name != Constants.ReduceKeyFieldName))
                {
                    // It seems like all the fields from the Map/Reduce results come out as strings
                    // so maybe this is a bit redundant??? Also it make the scripts harder, have to convert
                    // from string -> int, before doing Maths (if needed), i.e. parseFloat(value) + 5
                    //var numericField = field as NumericField;
                    //if (numericField != null)
                    //    values.Add(field.Name, numericField.NumericValue);
                    //else
                    //    values.Add(field.Name, field.StringValue);
                    values.Add(field.Name, field.StringValue);
                }
                var patchRequest = new ScriptedPatchRequest { Script = setupDoc.Script, Values = values };
                try
                {                    
                    var timer = Stopwatch.StartNew();
                    var result = scriptedJsonPatcher.Apply(resultDoc.DataAsJson, patchRequest);
                    /// TODO get rid of this wierdness, plus it would be better to not use the ref paramater
                    resultDoc = new JsonDocument() { DataAsJson = RavenJObject.FromObject(result) };
                    timer.Stop();
                    var msecs = timer.Elapsed.TotalMilliseconds;
                }
                catch (Exception e)
                {
                    log.WarnException("Could not process Indexed Properties script for " + resultDoc.Key + ", skipping this document", e);
                }
                return true;
            }

            private bool ApplySimpleFieldMappings(Document document, JsonDocument resultDoc, bool changesMade)
            {
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
                        throw new NotImplementedException();
                    }

					if (changesMade)
						database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);		
				}

				base.Dispose();
			}
		}
	}
}

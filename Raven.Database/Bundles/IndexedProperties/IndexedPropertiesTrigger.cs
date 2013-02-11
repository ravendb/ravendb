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
			return new IndexPropertyBatcher(Database, jsonSetupDoc, indexName, abstractViewGenerator);
		}

		public class IndexPropertyBatcher : AbstractIndexUpdateTriggerBatcher
		{
			private readonly DocumentDatabase database;
            private readonly JsonDocument rawSetupDoc;
            private readonly SetupDocInternal setupDoc;
			private readonly string indexName;
			private readonly AbstractViewGenerator viewGenerator;
            
            private readonly ConcurrentSet<string> itemsToRemove =
                new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

            public IndexPropertyBatcher(DocumentDatabase database, JsonDocument rawSetupDoc, string indexName, AbstractViewGenerator viewGenerator)
			{
				this.database = database;
				this.rawSetupDoc = rawSetupDoc;
				this.indexName = indexName;
				this.viewGenerator = viewGenerator;
                this.setupDoc = this.rawSetupDoc.DataAsJson.JsonDeserialization<SetupDocInternal>();
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
                    log.Warn("Null or empty \"entryKey\" provided, '{0}' for index '{1}'", setupDoc.DocumentKey, indexName);
                    return;
                }
                
				itemsToRemove.Add(entryKey);
			}

			public override void OnIndexEntryCreated(string entryKey, Document document)
			{
				var resultDocId = document.GetField(setupDoc.DocumentKey);
				if (resultDocId == null)
				{
					log.Warn("Could not find document id property '{0}' in '{1}' for index '{2}'", setupDoc.DocumentKey, entryKey, indexName);
					return;
				}

				var documentId = resultDocId.StringValue;

				itemsToRemove.TryRemove(documentId);

				var resultDoc = database.Get(documentId, null);
				if (resultDoc == null)
				{
					log.Warn("Could not find a document with the id '{0}' for index '{1}'", documentId, indexName);
					return;
				}

				var entityName = resultDoc.Metadata.Value<string>(Constants.RavenEntityName);
				if (entityName != null && viewGenerator.ForEntityNames.Contains(entityName))
				{
					log.Warn(
						"Rejected update for a potentially recursive update on document '{0}' because the index '{1}' includes documents with entity name of '{2}'",
						documentId, indexName, entityName);
					return;
				}
				if (viewGenerator.ForEntityNames.Count == 0)
				{
					log.Warn(
						"Rejected update for a potentially recursive update on document '{0}' because the index '{1}' includes all documents",
						documentId, indexName);
					return;
				}

				var changesMade = false;
                if (setupDoc.Type == IndexedPropertiesType.FieldMapping)                
                    changesMade = ApplySimpleFieldMappings(document, resultDoc);                
                else if (setupDoc.Type == IndexedPropertiesType.Scripted)                
                    changesMade = ApplyScript(document, resultDoc);
                
                if (changesMade)                
                    database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
			}

            private bool ApplyScript(Document document, JsonDocument resultDoc)
            {                                                
                // Make all the fields inside the Lucene doc, available as variables inside the script
                var fields = new Dictionary<string, object>();
                foreach (var field in document.GetFields()
                    .Where(f => f.Name.EndsWith("_Range") == false && f.Name != Constants.ReduceKeyFieldName))
                {
                    // It seems like all the fields from the Map/Reduce results come out as strings
                    // Try and confirm this, if so we don't need to worry about NumericField, StringField etc
                    fields.Add(field.Name, field.StringValue);
                }

                var changesMade = false;
                var scriptedJsonPatcher = new ScriptedJsonPatcher(database);

                // TODO fix this so we can namespace the values we pass in, so they can be accessed via "mapReduce." or "inputDoc."
                var values = new Dictionary<string, object> {{ "inputDoc", RavenJToken.FromObject(fields) }};
                var patchRequest = new ScriptedPatchRequest { Script = setupDoc.Script, Values = values };
                //var patchRequest = new ScriptedPatchRequest { Script = setupDoc.Script, Values = fields };
                try
                {                    
                    var timer = Stopwatch.StartNew();
                    var fieldNamesBefore = resultDoc.DataAsJson.Select(x => x.Key).ToList();
                    var result = scriptedJsonPatcher.Apply(resultDoc.DataAsJson, patchRequest);                    
                    resultDoc.DataAsJson = RavenJObject.FromObject(result);
                    timer.Stop();
                    var msecs = timer.Elapsed.TotalMilliseconds;
                    changesMade = true;                    

                    // Maybe we should do this once per batch, instead of one per index? Then we'd have fresher values?
                    // Is there ever a scenario when these fields could change, without the index changing?
                    if (setupDoc.ScriptFieldNames == null)
                    {                                             
                        // This is only the fields on the Lucene doc that is the input to the script, however we 
                        // really want the fields that out outputted from the script, i.e. the ones it creates
                        setupDoc.ScriptFieldNames = fields.Select(x => x.Key).ToList(); 

                        // How do we ensure that we only use the field names that were added?
                        // TODO fix this, it doesn't work because the fields can be present on the doc before we patch it!!
                        // To be accurate, we need a way for the patcher to tell us which items were written to during execution?
                        //setupDoc.ScriptFieldNames = resultDoc.DataAsJson.Select(x => x.Key).Except(fieldNamesBefore).ToList();

                        rawSetupDoc.DataAsJson = RavenJObject.FromObject(setupDoc);
                        database.Put(IndexedPropertiesSetupDoc.IdPrefix + indexName, rawSetupDoc.Etag, rawSetupDoc.DataAsJson, rawSetupDoc.Metadata, null);
                    }
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
				foreach (var documentId in itemsToRemove)
				{
					var resultDoc = database.Get(documentId, null);
					if (resultDoc == null)
					{
						log.Warn("Could not find a document with the id '{0}' for index '{1}", documentId, indexName);
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
                        if (setupDoc.ScriptFieldNames == null)
                            continue;

                        foreach (var field in setupDoc.ScriptFieldNames)                
                        {
                            if (resultDoc.DataAsJson.ContainsKey(field))
                            {
                                resultDoc.DataAsJson.Remove(field);
                                changesMade = true;
                            }
                        }
                    }

					if (changesMade)
						database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);		
				}

				base.Dispose();
			}
		}
	}

    internal class SetupDocInternal : IndexedPropertiesSetupDoc
    {
        public List<string> ScriptFieldNames { get; set; }
    }
}

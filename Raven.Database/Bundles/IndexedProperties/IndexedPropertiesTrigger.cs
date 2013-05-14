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
			private readonly IndexedPropertiesSetupDoc setupDoc;
			private readonly string indexName;
			private readonly AbstractViewGenerator viewGenerator;
			private readonly ConcurrentSet<string> itemsToRemove = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

            private readonly ConcurrentSet<string> itemsToRemove =
                new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

            public IndexPropertyBatcher(DocumentDatabase database, JsonDocument rawSetupDoc, string indexName, AbstractViewGenerator viewGenerator)
			{
				this.database = database;
				this.rawSetupDoc = rawSetupDoc;
				this.indexName = indexName;
				this.viewGenerator = viewGenerator;
                if (rawSetupDoc != null && rawSetupDoc.DataAsJson != null)
                    this.setupDoc = this.rawSetupDoc.DataAsJson.JsonDeserialization<IndexedPropertiesSetupDoc>();
			}

			public override void OnIndexEntryDeleted(string entryKey)
			{
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

                if (setupDoc.Type == IndexedPropertiesType.FieldMapping)
                {
                    var changesMade = ApplySimpleFieldMappings(document, resultDoc);
                    if (changesMade)
                        database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
                }
                else if (setupDoc.Type == IndexedPropertiesType.Scripted)
                {
                    // We don't store any docs here, it's upto the script to use Put if it wants to
                    ApplyScript(document, resultDoc);
                }                                
			}

            private void ApplyScript(Document document, JsonDocument resultDoc)
            {                                                
                // Make all the fields from inside the Lucene doc, available as variables inside the script
                var fields = new Dictionary<string, object>();
                foreach (var field in document.GetFields()
                    .Where(f => f.Name.EndsWith("_Range") == false && f.Name != Constants.ReduceKeyFieldName))
                {
                    // It seems like all the fields from the Map/Reduce results come out as strings
                    // Try and confirm this, if so we don't need to worry about NumericField, StringField etc
                    fields.Add(field.Name, field.StringValue);
                }
                
                var scriptedJsonPatcher = new ScriptedJsonPatcher(database);
                
                var values = new Dictionary<string, object> {
                    { "result", fields },
                    { "metadata", resultDoc.Metadata }
                };
                var patchRequest = new ScriptedPatchRequest { Script = setupDoc.Script, Values = values };                                
                try
                {                                                            
                    var result = scriptedJsonPatcher.Apply(resultDoc.DataAsJson, patchRequest);                    
                    resultDoc.DataAsJson = RavenJObject.FromObject(result);                    
                }
                catch (Exception e)
                {
                    log.WarnException("Could not process Indexed Properties script for " + resultDoc.Key + ", skipping this document", e);
                }               
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
					else if(field.IsBinary == false)
					{
						string stringValue = GetStringValue(field);
						try
						{
							resultDoc.DataAsJson[mapping.Value] = RavenJToken.Parse(stringValue);
						}
						catch
					{
							resultDoc.DataAsJson[mapping.Value] = stringValue;
						}
					}
					changesMade = true;
				}
                return changesMade;
			}

			private static string GetStringValue(IFieldable field)
			{
				switch (field.StringValue)
				{
					case Constants.NullValue:
						return null;
					case Constants.EmptyString:
						return string.Empty;
					default:
						return field.StringValue;
				}
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
                        if (setupDoc.CleanupScript == null)
                            continue;

                        var scriptedJsonPatcher = new ScriptedJsonPatcher(database);

                        var values = new Dictionary<string, object> { { "deleteDocId", documentId } };
                        var patchRequest = new ScriptedPatchRequest { Script = setupDoc.CleanupScript, Values = values };
                        try
                        {                            
                            var result = scriptedJsonPatcher.Apply(resultDoc.DataAsJson, patchRequest);
                            resultDoc.DataAsJson = RavenJObject.FromObject(result);                         
                        }
                        catch (Exception e)
                        {
                            log.WarnException("Could not process Indexed Properties script for " + resultDoc.Key + ", skipping this document", e);
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

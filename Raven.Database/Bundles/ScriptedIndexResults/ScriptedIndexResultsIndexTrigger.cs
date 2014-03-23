// -----------------------------------------------------------------------
//  <copyright file="ScriptedIndexResultsIndexTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.ScriptedIndexResults
{
    using System.Linq;

    [InheritedExport(typeof(AbstractIndexUpdateTrigger))]
    [ExportMetadata("Bundle", "ScriptedIndexResults")]
    public class ScriptedIndexResultsIndexTrigger : AbstractIndexUpdateTrigger
    {
        private static ILog log = LogManager.GetCurrentClassLogger();

        public override AbstractIndexUpdateTriggerBatcher CreateBatcher(int indexId)
        {
            //Only apply the trigger if there is a setup doc for this particular index
            Index indexInstance = this.Database.IndexStorage.GetIndexInstance(indexId);
            if (indexInstance == null)
                return null;
            var jsonSetupDoc = Database.Documents.Get(Abstractions.Data.ScriptedIndexResults.IdPrefix + indexInstance.PublicName, null);
            if (jsonSetupDoc == null)
                return null;
            var scriptedIndexResults = jsonSetupDoc.DataAsJson.JsonDeserialization<Abstractions.Data.ScriptedIndexResults>();
            var abstractViewGenerator = Database.IndexDefinitionStorage.GetViewGenerator(indexId);
            if (abstractViewGenerator == null)
                throw new InvalidOperationException("Could not find view generator for: " + indexInstance.PublicName);
            scriptedIndexResults.Id = indexInstance.PublicName;
            return new Batcher(Database, scriptedIndexResults, abstractViewGenerator.ForEntityNames);
        }

        public class Batcher : AbstractIndexUpdateTriggerBatcher
        {
            private readonly DocumentDatabase database;
            private readonly Abstractions.Data.ScriptedIndexResults scriptedIndexResults;
            private readonly HashSet<string> forEntityNames;
            
            private readonly Dictionary<string, List<RavenJObject>> created = new Dictionary<string, List<RavenJObject>>(StringComparer.InvariantCultureIgnoreCase);
            private readonly Dictionary<string, List<RavenJObject>> removed = new Dictionary<string, List<RavenJObject>>(StringComparer.InvariantCultureIgnoreCase);

            public Batcher(DocumentDatabase database, Abstractions.Data.ScriptedIndexResults scriptedIndexResults, HashSet<string> forEntityNames)
            {
                this.database = database;
                this.scriptedIndexResults = scriptedIndexResults;
                this.forEntityNames = forEntityNames;
            }

            public override bool RequiresDocumentOnIndexEntryDeleted { get { return true;  } }

            public override void OnIndexEntryCreated(string entryKey, Document document)
            {
                if (created.ContainsKey(entryKey) == false)
                {
                    created[entryKey] = new List<RavenJObject>();
                }
                created[entryKey].Add(CreateJsonDocumentFromLuceneDocument(document));
            }

            public override void OnIndexEntryDeleted(string entryKey, Document document = null)
            {
                if (removed.ContainsKey(entryKey) == false)
                {
                    removed[entryKey] = new List<RavenJObject>();
                }
                removed[entryKey].Add(document != null ? CreateJsonDocumentFromLuceneDocument(document) : new RavenJObject());
            }

            public override void Dispose()
            {
                var patcher = new ScriptedIndexResultsJsonPatcher(database, forEntityNames);

                if (string.IsNullOrEmpty(scriptedIndexResults.DeleteScript) == false)
                {
                    foreach (var kvp in removed)
                    {
                        foreach (var entry in kvp.Value)
                        {
                            patcher.Apply(entry, new ScriptedPatchRequest
                            {
                                Script = scriptedIndexResults.DeleteScript,
                                Values =
                                {
                                    {"key", kvp.Key}
                                }
                            });

                            if (log.IsDebugEnabled && patcher.Debug.Count > 0)
                            {
                                log.Debug("Debug output for doc: {0} for index {1} (delete):\r\n.{2}", kvp.Key,
                                          scriptedIndexResults.Id, string.Join("\r\n", patcher.Debug));

                                patcher.Debug.Clear();
                            }
                        }

                    }
                }

                if (string.IsNullOrEmpty(scriptedIndexResults.IndexScript) == false)
                {
                    foreach (var kvp in created)
                    {
                        try
                        {
                            foreach (var entry in kvp.Value)
                            {
                                patcher.Apply(entry, new ScriptedPatchRequest
                                {
                                    Script = scriptedIndexResults.IndexScript,
                                    Values =
                                {
                                    {"key", kvp.Key}
                                }
                                });
                            }
                           
                        }
                        catch (Exception e)
                        {
                            log.Warn(
                                "Could not apply index script " + scriptedIndexResults.Id +
                                " to index result with key: " + kvp.Key, e);
                        }
                        finally
                        {
                            if (log.IsDebugEnabled && patcher.Debug.Count > 0)
                            {
                                log.Debug("Debug output for doc: {0} for index {1} (index):\r\n.{2}", kvp.Key, scriptedIndexResults.Id, string.Join("\r\n", patcher.Debug));

                                patcher.Debug.Clear();
                            }
                        }
                    }
                }

                database.TransactionalStorage.Batch(accessor =>
                {
                    foreach (var operation in patcher.GetOperations())
                    {
	                    switch (operation.Type)
	                    {
							case ScriptedJsonPatcher.OperationType.Put:
			                    database.Documents.Put(operation.Document.Key, operation.Document.Etag, operation.Document.DataAsJson,
			                                 operation.Document.Metadata, null);
								break;
							case ScriptedJsonPatcher.OperationType.Delete:
								database.Documents.Delete(operation.DocumentKey, null, null);
								break;
							default:
								throw new ArgumentOutOfRangeException("operation.Type");
	                    }    
                    }
                });
            }

            public class ScriptedIndexResultsJsonPatcher : ScriptedJsonPatcher
            {
                private readonly HashSet<string> entityNames;

                public ScriptedIndexResultsJsonPatcher(DocumentDatabase database, HashSet<string> entityNames)
                    : base(database)
                {
                    this.entityNames = entityNames;
                }

                protected override void CustomizeEngine(Jint.JintEngine jintEngine)
                {
                    jintEngine.SetFunction("DeleteDocument", ((Action<string>)(DeleteFromContext)));
                }

                protected override void RemoveEngineCustomizations(Jint.JintEngine jintEngine)
                {
                    jintEngine.RemoveParameter("DeleteDocument");
                }

                protected override void ValidateDocument(JsonDocument newDocument)
                {
                    if (newDocument.Metadata == null)
                        return;
                    var entityName = newDocument.Metadata.Value<string>(Constants.RavenEntityName);
                    if (string.IsNullOrEmpty(entityName))
                    {
                        if (entityNames.Count == 0)
                            throw new InvalidOperationException(
                                "Invalid Script Index Results Recursion!\r\n"+
                                "The scripted index result doesn't have an entity name, but the index apply to all documents.\r\n" +
                                "Scripted Index Results cannot create documents that will be indexed by the same document that created them, " +
                                "since that would create a infinite loop of indexing/creating documents.");
                        return;
                    }
                    if (entityNames.Contains(entityName))
                    {
                        throw new InvalidOperationException(
                            "Invalid Script Index Results Recursion!\r\n" +
                            "The scripted index result have an entity name of "+entityName + ", but the index apply to documents with that entity name.\r\n" +
                            "Scripted Index Results cannot create documents that will be indexed by the same document that created them, " +
                            "since that would create a infinite loop of indexing/creating documents.");
                    }
                }
            }

            private static RavenJObject CreateJsonDocumentFromLuceneDocument(Document document)
            {
                var field = document.GetField(Constants.ReduceValueFieldName);
                if (field != null)
                    return RavenJObject.Parse(field.StringValue);

                var ravenJObject = new RavenJObject();

                var fields = document.GetFields();
                var arrayMarkers = fields
                    .Where(x => x.Name.EndsWith("_IsArray"))
                    .Select(x => x.Name)
                    .ToList();

                foreach (var fieldable in fields)
                {
                    var stringValue = GetStringValue(fieldable);
                    var isArrayMarker = fieldable.Name.EndsWith("_IsArray");
                    var isArray = !isArrayMarker && arrayMarkers.Contains(fieldable.Name + "_IsArray");

                    RavenJToken token;
                    var isJson = RavenJToken.TryParse(stringValue, out token);

                    RavenJToken value;
                    if (ravenJObject.TryGetValue(fieldable.Name, out value) == false)
                    {
                        if (isArray)
                            ravenJObject[fieldable.Name] = new RavenJArray { isJson ? token : stringValue };
                        else if (isArrayMarker)
                        {
                            var fieldName = fieldable.Name.Substring(0, fieldable.Name.Length - 8);
                            ravenJObject[fieldable.Name] = isJson ? token : stringValue;
                            ravenJObject[fieldName] = new RavenJArray();
                        }
                        else
                            ravenJObject[fieldable.Name] = isJson ? token : stringValue;
                    }
                    else
                    {
                        var ravenJArray = value as RavenJArray;
                        if (ravenJArray != null)
                            ravenJArray.Add(isJson ? token : stringValue);
                        else
                        {
                            ravenJArray = new RavenJArray { value, isJson ? token : stringValue };
                            ravenJObject[fieldable.Name] = ravenJArray;
                        }
                    }
                }
                return ravenJObject;
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
        }
    }
}
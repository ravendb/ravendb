// -----------------------------------------------------------------------
//  <copyright file="ScriptedIndexResultsIndexTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Threading;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
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
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        public override AbstractIndexUpdateTriggerBatcher CreateBatcher(int indexId)
        {
            //Only apply the trigger if there is a setup doc for this particular index
            Index indexInstance = Database.IndexStorage.GetIndexInstance(indexId);
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
            private static readonly ILog Log = LogManager.GetCurrentClassLogger();

            private readonly DocumentDatabase database;
            private readonly Abstractions.Data.ScriptedIndexResults scriptedIndexResults;
            private readonly HashSet<string> forEntityNames;
            private readonly bool hasPutScript;
            private readonly bool hasDeleteScript;

            private readonly ConcurrentDictionary<string, ConcurrentBag<RavenJObject>> created = new ConcurrentDictionary<string, ConcurrentBag<RavenJObject>>(StringComparer.InvariantCultureIgnoreCase);
            private readonly ConcurrentDictionary<string, ConcurrentBag<RavenJObject>> removed = new ConcurrentDictionary<string, ConcurrentBag<RavenJObject>>(StringComparer.InvariantCultureIgnoreCase);

            public Batcher(DocumentDatabase database, Abstractions.Data.ScriptedIndexResults scriptedIndexResults, HashSet<string> forEntityNames)
            {
                this.database = database;
                this.scriptedIndexResults = scriptedIndexResults;
                this.forEntityNames = forEntityNames;
                hasPutScript = string.IsNullOrWhiteSpace(scriptedIndexResults.IndexScript) == false;
                hasDeleteScript = string.IsNullOrWhiteSpace(scriptedIndexResults.DeleteScript) == false;

                if (Log.IsDebugEnabled)
                    Log.Debug("Created ScriptedIndexResultsBatcher for {0}", scriptedIndexResults.Id);
            }

            public override bool RequiresDocumentOnIndexEntryDeleted { get { return true; } }

            public override void OnIndexEntryCreated(string entryKey, Document document)
            {
                if (hasPutScript == false)
                    return;

                //precaution, should never happen
                if (string.IsNullOrWhiteSpace(entryKey))
                    throw new ArgumentNullException("entryKey");

                var bag = created.GetOrAdd(entryKey, _ => new ConcurrentBag<RavenJObject>());

                if (Log.IsDebugEnabled)
                    Log.Debug("Schedule create with key {0} for scripted index results {1}", entryKey, scriptedIndexResults.Id);

                bag.Add(CreateJsonDocumentFromLuceneDocument(document));
            }

            public override void OnIndexEntryDeleted(string entryKey, Document document = null)
            {
                if (hasDeleteScript == false)
                    return;

                var bag = removed.GetOrAdd(entryKey, _ => new ConcurrentBag<RavenJObject>());

                if (Log.IsDebugEnabled)
                    Log.Debug("Schedule delete with key {0} for scripted index results {1}", entryKey, scriptedIndexResults.Id);

                bag.Add(document != null ? CreateJsonDocumentFromLuceneDocument(document) : new RavenJObject());
            }

            public override void Dispose()
            {
                if (hasDeleteScript == false && hasPutScript == false)
                    return;

                var shouldRetry = false;
                var retries = 128;
                Random rand = null;

                do
                {
                    using (shouldRetry ? database.TransactionalStorage.DisableBatchNesting() : null)
                    {
                        var patcher = new ScriptedJsonPatcher(database);
                        using (var scope = new ScriptedIndexResultsJsonPatcherScope(database, forEntityNames))
                        {
                            if (hasDeleteScript)
                            {
                                foreach (var kvp in removed)
                                {
                                    foreach (var entry in kvp.Value)
                                    {
                                        try
                                        {
                                            patcher.Apply(scope, entry, new ScriptedPatchRequest
                                            {
                                                Script = scriptedIndexResults.DeleteScript,
                                                Values =
                                                {
                                                    {
                                                        "key", kvp.Key
                                                    }
                                                }
                                            });
                                        }
                                        catch (Exception e)
                                        {
                                            Log.WarnException("Could not apply delete script " + scriptedIndexResults.Id + " to index result with key: " + kvp.Key, e);
                                        }
                                        finally
                                        {
                                            if (Log.IsDebugEnabled && patcher.Debug.Count > 0)
                                            {
                                                Log.Debug("Debug output for doc: {0} for index {1} (delete):\r\n.{2}", kvp.Key, scriptedIndexResults.Id, string.Join("\r\n", patcher.Debug));

                                                patcher.Debug.Clear();
                                            }
                                        }
                                    }

                                }
                            }

                            if (hasPutScript)
                            {
                                foreach (var kvp in created)
                                {
                                    try
                                    {
                                        foreach (var entry in kvp.Value)
                                        {
                                            patcher.Apply(scope, entry, new ScriptedPatchRequest
                                            {
                                                Script = scriptedIndexResults.IndexScript,
                                                Values =
                                                {
                                                    {
                                                        "key", kvp.Key
                                                    }
                                                }
                                            });
                                        }

                                    }
                                    catch (Exception e)
                                    {
                                        Log.WarnException("Could not apply index script " + scriptedIndexResults.Id + " to index result with key: " + kvp.Key, e);
                                    }
                                    finally
                                    {
                                        if (Log.IsDebugEnabled && patcher.Debug.Count > 0)
                                        {
                                            Log.Debug("Debug output for doc: {0} for index {1} (index):\r\n.{2}", kvp.Key, scriptedIndexResults.Id, string.Join("\r\n", patcher.Debug));

                                            patcher.Debug.Clear();
                                        }
                                    }
                                }
                            }

                            try
                            {
                                database.TransactionalStorage.Batch(accessor =>
                                {
                                    foreach (var operation in scope.GetOperations())
                                    {
                                        switch (operation.Type)
                                        {
                                            case ScriptedJsonPatcher.OperationType.Put:
                                                if (Log.IsDebugEnabled)
                                                    Log.Debug("Perform PUT on {0} for scripted index results {1}", operation.Document.Key, scriptedIndexResults.Id);

                                                database.Documents.Put(operation.Document.Key, operation.Document.Etag, operation.Document.DataAsJson, operation.Document.Metadata, null);
                                                break;
                                            case ScriptedJsonPatcher.OperationType.Delete:
                                                if (Log.IsDebugEnabled)
                                                    Log.Debug("Perform DELETE on {0} for scripted index results {1}", operation.DocumentKey, scriptedIndexResults.Id);

                                                database.Documents.Delete(operation.DocumentKey, null, null);
                                                break;
                                            default:
                                                throw new ArgumentOutOfRangeException("operation.Type: " + operation.Type);
                                        }
                                    }
                                });

                                shouldRetry = false;
                            }
                            catch (ConcurrencyException ex)
                            {
                                if (scriptedIndexResults.RetryOnConcurrencyExceptions && retries-- > 0)
                                {
                                    shouldRetry = true;
                                    if (rand == null)
                                        rand = new Random();

                                    if (Log.IsDebugEnabled)
                                        Log.DebugException(string.Format("Applying PUT/DELETE for scripted index results {0} failed with concurrency exception. Retrying", scriptedIndexResults.Id), ex);

                                    Thread.Sleep(rand.Next(5, Math.Max(retries * 2, 10)));

                                    continue;
                                }

                                if (Log.IsDebugEnabled)
                                    Log.DebugException(string.Format("Applying PUT/DELETE for scripted index results {0} failed with concurrency exception {1} times.", scriptedIndexResults.Id, 128 - retries + 1), ex);

                                throw;
                            }
                        }
                    }
                } while (shouldRetry);
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
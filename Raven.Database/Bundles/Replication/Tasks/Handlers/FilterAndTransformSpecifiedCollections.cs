// -----------------------------------------------------------------------
//  <copyright file="PatchReplicatedDocs.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Tasks.Handlers
{
    public class FilterAndTransformSpecifiedCollections : IReplicatedDocsHandler
    {
        private readonly static ILog Log = LogManager.GetCurrentClassLogger();

        private readonly DocumentDatabase database;
        private readonly ReplicationStrategy strategy;
        private readonly string destinationId;

        public FilterAndTransformSpecifiedCollections(DocumentDatabase database, ReplicationStrategy strategy, string destinationId)
        {
            this.database = database;
            this.strategy = strategy;
            this.destinationId = destinationId;
        }

        public IEnumerable<JsonDocument> Handle(IEnumerable<JsonDocument> docs)
        {
            if (strategy.SpecifiedCollections == null || strategy.SpecifiedCollections.Count == 0)
                return docs;

            return docs.Select(doc =>
            {
                var collection = doc.Metadata.Value<string>(Constants.RavenEntityName);

                string script;
                if (string.IsNullOrEmpty(collection) || strategy.SpecifiedCollections.TryGetValue(collection, out script) == false)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug(string.Format("Will not replicate document '{0}' to '{1}' because the replication of specified collection is turned on while the document does not belong to any of them", doc.Key, destinationId));
                    return null;
                }

                if (script == null || doc.Metadata.ContainsKey(Constants.RavenDeleteMarker))
                    return doc;

                var scriptedPatchRequest = new ScriptedPatchRequest
                {
                    Script = script
                };

                var patcher = new ReplicationScriptedJsonPatcher(database, scriptedPatchRequest);
                using (var scope = new DefaultScriptedJsonPatcherOperationScope(database))
                {
                    try
                    {
                        
                        var transformedDoc = patcher.Apply(scope, doc.ToJson(), scriptedPatchRequest, doc.SerializedSizeOnDisk);

                        if (scope.ActualPatchResult == JsValue.Null) // null means that document should be skip
                        {
                            if (Log.IsDebugEnabled)
                                Log.Debug(string.Format("Will not replicate document '{0}' to '{1}' because a collection specific script filtered it out", doc.Key, destinationId));
                            return null;
                        }

                        doc.Metadata = (RavenJObject) transformedDoc[Constants.Metadata];
                        transformedDoc.Remove(Constants.Metadata);

                        doc.DataAsJson = transformedDoc;

                        return doc;
                    }
                    catch (ParseException e)
                    {
                        Log.WarnException(string.Format("Could not parse replication transformation script of '{0}' collection on document {1}", collection, doc.Key), e);

                        throw;
                    }
                    catch (Exception e)
                    {
                        Log.WarnException(string.Format("Could not apply replication transformation script of '{0}' collection on document {1}", collection, doc.Key), e);

                        throw;
                    }
                }
            })
            .Where(x => x != null);
        }
    }
}

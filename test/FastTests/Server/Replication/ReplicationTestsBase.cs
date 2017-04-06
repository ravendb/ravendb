using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Xunit;
using Xunit.Sdk;

namespace FastTests.Server.Replication
{
    public class ReplicationTestsBase : RavenTestBase
    {

        protected List<object> GetRevisions(DocumentStore store,string id)
        {
            using (var commands = store.Commands())
            {
                var command = new GetRevisionsCommand(id);

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected void EnsureReplicating(DocumentStore src, DocumentStore dst)
        {
            var id = "marker/" + Guid.NewGuid().ToString();
            using (var s = src.OpenSession())
            {
                s.Store(new { }, id);
                s.SaveChanges();
            }
            WaitForDocumentToReplicate<object>(dst, id, 15 * 1000);
        }


        protected Dictionary<string, string[]> GetConnectionFaliures(DocumentStore store)
        {
            using (var commands = store.Commands())
            {
                var command = new GetConncectionFailuresCommand();

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected GetConflictsResult GetConflicts(DocumentStore store, string docId)
        {
            using (var commands = store.Commands())
            {
                var command = new GetReplicationConflictsCommand(docId);

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected GetConflictsResult WaitUntilHasConflict(DocumentStore store, string docId, int count = 2)
        {
            int timeout = 5000;

            if (Debugger.IsAttached)
                timeout *= 100;
            GetConflictsResult conflicts;
            var sw = Stopwatch.StartNew();
            do
            {
                conflicts = GetConflicts(store, docId);

                if (conflicts.Results.Length >= count)
                    break;

                if (sw.ElapsedMilliseconds > timeout)
                {
                    throw new XunitException($"Timed out while waiting for conflicts on {docId} we have {conflicts.Results.Length} conflicts " +
                                             $"on database {store.DefaultDatabase}");
                }
            } while (true);

            return conflicts;
        }

        protected bool WaitForDocumentDeletion(DocumentStore store,
            string docId,
            int timeout = 10000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession())
                {
                    try
                    {
                        var doc = session.Load<dynamic>(docId);
                        if (doc == null)
                            return true;
                    }
                    catch (ConflictException)
                    {
                        // expected that we might get conflict, ignore and wait
                    }
                }
            }
            using (var session = store.OpenSession())
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<dynamic>(docId);
                if (doc == null)
                    return true;
            }
            return false;
        }

        protected bool WaitForDocument<T>(DocumentStore store,
            string docId,
            Func<T, bool> predicate,
            int timeout = 10000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession())
                {
                    try
                    {
                        var doc = session.Load<T>(docId);
                        if (doc != null && predicate(doc))
                            return true;
                    }
                    catch (ConflictException)
                    {
                        // expected that we might get conflict, ignore and wait
                    }
                }
            }
            using (var session = store.OpenSession())
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<T>(docId);
                if (doc != null && predicate(doc))
                    return true;
            }
            return false;
        }

        protected bool WaitForDocument(DocumentStore store,
            string docId,
            int timeout = 10000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession())
                {
                    try
                    {
                        var doc = session.Load<dynamic>(docId);
                        if (doc != null)
                            return true;
                    }
                    catch (DocumentConflictException)
                    {
                        // expected that we might get conflict, ignore and wait
                    }
                }
            }
            using (var session = store.OpenSession())
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<dynamic>(docId);
                if (doc != null)
                    return true;
            }
            return false;
        }

        protected T WaitForValue<T>(Func<T> act, T expectedVal)
        {
            int timeout = 10000;
            if (Debugger.IsAttached)
                timeout *= 100;
            var sw = Stopwatch.StartNew();
            do
            {
                var currentVal = act();
                if (expectedVal.Equals(currentVal))
                {
                    return currentVal;
                }
                if (sw.ElapsedMilliseconds > timeout)
                {
                    return currentVal;
                }
                Thread.Sleep(16);
            } while (true);
        }

        protected List<string> WaitUntilHasTombstones(
                DocumentStore store,
                int count = 1)
        {

            int timeout = 5000;
            if (Debugger.IsAttached)
                timeout *= 100;
            List<string> tombstones;
            var sw = Stopwatch.StartNew();
            do
            {
                tombstones = GetTombstones(store);

                if (tombstones == null ||
                    tombstones.Count >= count)
                    break;

                if (sw.ElapsedMilliseconds > timeout)
                {
                    Assert.False(true, store.Identifier + " -> Timed out while waiting for tombstones, we have " + tombstones.Count + " tombstones, but should have " + count);
                }

            } while (true);
            return tombstones ?? new List<string>();
        }


        protected List<string> GetTombstones(DocumentStore store)
        {
            using (var commands = store.Commands())
            {
                var command = new GetReplicationTombstonesCommand();

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected FullTopologyInfo GetFullTopology(DocumentStore store)
        {
            using (var commands = store.Commands())
            {
                var command = new GetFullTopologyCommand();

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected T WaitForDocumentToReplicate<T>(DocumentStore store, string id, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenSession(store.DefaultDatabase))
                {
                    var doc = session.Load<T>(id);
                    if (doc != null)
                        return doc;
                }
            }

            return default(T);
        }

        public static void SetScriptResolution(DocumentStore store, string script, string collection)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new ReplicationDocument
                {
                    DocumentConflictResolution = StraightforwardConflictResolution.None,
                    ResolveByCollection = new Dictionary<string, ScriptResolver>{
                            { collection, new ScriptResolver
                                {
                                    Script = script
                                }
                            }
                        }
                }, Constants.Documents.Replication.ReplicationConfigurationDocument);

                session.SaveChanges();
            }
        }

        protected static void SetReplicationConflictResolution(DocumentStore store,
            StraightforwardConflictResolution conflictResolution)
        {
            using (var session = store.OpenSession())
            {
                var destinations = new List<ReplicationDestination>();
                session.Store(new ReplicationDocument
                {
                    Destinations = destinations,
                    DocumentConflictResolution = conflictResolution
                }, Constants.Documents.Replication.ReplicationConfigurationDocument);
                session.SaveChanges();
            }
        }

        protected static void SetupReplication(DocumentStore fromStore, StraightforwardConflictResolution builtinConflictResolution = StraightforwardConflictResolution.None, params DocumentStore[] toStores)
        {
            using (var session = fromStore.OpenSession())
            {
                var destinations = new List<ReplicationDestination>();
                foreach (var store in toStores)
                    destinations.Add(
                        new ReplicationDestination
                        {
                            Database = store.DefaultDatabase,
                            Url = store.Url,

                        });
                session.Store(new ReplicationDocument
                {
                    Destinations = destinations,
                    DocumentConflictResolution = builtinConflictResolution
                }, Constants.Documents.Replication.ReplicationConfigurationDocument);
                session.SaveChanges();
            }
        }

        protected static void SetupReplication(DocumentStore fromStore, Dictionary<string, string> etlScripts, params DocumentStore[] toStores)
        {
            using (var session = fromStore.OpenSession())
            {
                var destinations = new List<ReplicationDestination>();
                foreach (var store in toStores)
                    destinations.Add(
                        new ReplicationDestination
                        {
                            Database = store.DefaultDatabase,
                            Url = store.Url,
                            SpecifiedCollections = etlScripts
                        });
                session.Store(new ReplicationDocument
                {
                    Destinations = destinations
                }, Constants.Documents.Replication.ReplicationConfigurationDocument);
                session.SaveChanges();
            }
        }


        protected void SetupReplication(DocumentStore fromStore, params DocumentStore[] toStores)
        {
            SetupReplication(fromStore,
                new ReplicationDocument
                {

                },
                toStores);
        }

        protected void SetupReplication(DocumentStore fromStore, ReplicationDocument configOptions, params DocumentStore[] toStores)
        {
            using (var session = fromStore.OpenSession())
            {
                var destinations = new List<ReplicationDestination>();
                foreach (var store in toStores)
                {
                    var replicationDestination = new ReplicationDestination
                    {
                        Database = store.DefaultDatabase,
                        Url = store.Url
                    };
                    ModifyReplicationDestination(replicationDestination);
                    destinations.Add(replicationDestination);
                }

                configOptions.Destinations = destinations;
                session.Store(configOptions, Constants.Documents.Replication.ReplicationConfigurationDocument);
                session.SaveChanges();
            }
        }

        protected static void DeleteReplication(DocumentStore fromStore, DocumentStore deletedStoreDestination)
        {
            ReplicationDocument replicationConfigDocument;

            using (var session = fromStore.OpenSession())
            {
                replicationConfigDocument =
                    session.Load<ReplicationDocument>(Constants.Documents.Replication.ReplicationConfigurationDocument);

                if (replicationConfigDocument == null)
                    return;

                session.Delete(replicationConfigDocument);
                session.SaveChanges();
            }

            using (var session = fromStore.OpenSession())
            {

                replicationConfigDocument.Destinations.RemoveAll(
                    x => x.Database == deletedStoreDestination.DefaultDatabase);

                session.Store(replicationConfigDocument);
                session.SaveChanges();
            }
        }

        protected virtual void ModifyReplicationDestination(ReplicationDestination replicationDestination)
        {
        }

        protected static void SetupReplicationWithCustomDestinations(DocumentStore fromStore, params ReplicationDestination[] toDestinations)
        {
            using (var session = fromStore.OpenSession())
            {
                session.Store(new ReplicationDocument
                {
                    Destinations = toDestinations.ToList()
                }, Constants.Documents.Replication.ReplicationConfigurationDocument);
                session.SaveChanges();
            }
        }
        
        private class GetRevisionsCommand : RavenCommand<List<object>>
        {
            private readonly string _id;

            public GetRevisionsCommand(string id)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                _id = id;
            }
            public override bool IsReadRequest => true;
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/revisions?key={_id}";

                ResponseType = RavenCommandResponseType.Object;
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                BlittableJsonReaderArray array;
                if (response.TryGet("Results", out array) == false)
                    ThrowInvalidResponse();

                var result = new List<object>();
                foreach (var arrayItem in array.Items)
                {
                    result.Add(arrayItem);
                }

                Result = result;
            }
        }

        private class GetConncectionFailuresCommand : RavenCommand<Dictionary<string, string[]>>
        {

            public override bool IsReadRequest => true;
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/debug/incoming-rejection-info";

                ResponseType = RavenCommandResponseType.Array;
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }
            public override void SetResponse(BlittableJsonReaderArray response, bool fromCache)
            {
                List<string> list = new List<string>();
                Dictionary<string, string[]> result = new Dictionary<string, string[]>();
                foreach (BlittableJsonReaderObject responseItem in response.Items)
                {
                    BlittableJsonReaderObject obj;
                    responseItem.TryGet("Key", out obj);
                    string name;
                    obj.TryGet("SourceDatabaseName", out name);

                    BlittableJsonReaderArray arr;
                    responseItem.TryGet("Value", out arr);
                    list.Clear();
                    foreach (BlittableJsonReaderObject arrItem in arr)
                    {
                        string reason;
                        arrItem.TryGet("Reason", out reason);
                        list.Add(reason);
                    }
                    result.Add(name, list.ToArray());
                }
                Result = result;
            }
            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                throw new NotImplementedException();
            }
        }

        private class GetFullTopologyCommand : RavenCommand<FullTopologyInfo>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/topology/full";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.FullTopologyInfo(response);
            }
        }

        private class GetReplicationTombstonesCommand : RavenCommand<List<string>>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/tombstones";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                BlittableJsonReaderArray array;
                if (response.TryGet("Results", out array) == false)
                    ThrowInvalidResponse();

                var result = new List<string>();
                foreach (BlittableJsonReaderObject json in array)
                {
                    string key;
                    if (json.TryGet("Key", out key) == false)
                        ThrowInvalidResponse();

                    result.Add(key);
                }

                Result = result;
            }
        }

        private class GetReplicationConflictsCommand : RavenCommand<GetConflictsResult>
        {
            private readonly string _id;

            public GetReplicationConflictsCommand(string id)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                _id = id;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/conflicts?docId={_id}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();               

                Result = JsonDeserializationClient.GetConflictsResult(response);
            }
        }
    }
}
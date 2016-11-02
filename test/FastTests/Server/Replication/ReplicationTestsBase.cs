using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Replication.Messages;
using Raven.Json.Linq;
using Raven.Server.Extensions;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationTestsBase : RavenTestBase
    {
        protected async Task<Dictionary<string, List<ChangeVectorEntry[]>>> GetConflicts(DocumentStore store,
    string docId)
        {
            var url = $"{store.Url}/databases/{store.DefaultDatabase}/replication/conflicts?docId={docId}";
            using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
            {
                request.ExecuteRequest();
                var conflictsJson = RavenJArray.Parse(await request.Response.Content.ReadAsStringAsync());				
                var conflicts = conflictsJson.Select(x => new
                {
                    Key = x.Value<string>("Key"),
                    ChangeVector = x.Value<RavenJArray>("ChangeVector").Select(c => c.FromJson()).ToArray()
                }).GroupBy(x => x.Key).ToDictionary(x => x.Key, x => x.Select(i => i.ChangeVector).ToList());

                return conflicts;
            }
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
                    var doc = session.Load<dynamic>(docId);
                    if (doc == null)
                        return true;
                }
                Thread.Sleep(10);
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
                    var doc = session.Load<dynamic>(docId);
                    if (doc != null)
                        return true;
                }
                Thread.Sleep(10);
            }

            return false;
        }

        protected async Task<List<string>> WaitUntilHasTombstones(
                DocumentStore store,
                int count = 1,
                int timeout = 4000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;
            List<string> tombstones;
            var sw = Stopwatch.StartNew();
            do
            {
                tombstones = await GetTombstones(store);

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


        protected async Task<List<string>> GetTombstones(DocumentStore store)
        {
            var url = $"{store.Url}/databases/{store.DefaultDatabase}/replication/tombstones";
            using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
            {
                request.ExecuteRequest();
                var tombstonesJson = RavenJArray.Parse(await request.Response.Content.ReadAsStringAsync());
                var tombstones = tombstonesJson.Select(x => x.Value<string>("Key")).ToList();
                return tombstones;
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
                Thread.Sleep(25);
            }

            return default(T);
        }

        protected bool WaitForIndexToReplicate(DocumentStore store, string indexName, int timeout)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                var indexInformation = store.DatabaseCommands.GetStatistics()
                                          .Indexes.FirstOrDefault(x => 
                                                x.Name.Equals(indexName, StringComparison.CurrentCultureIgnoreCase));

                if (indexInformation != null)
                    return true;

                Thread.Sleep(25);
            }

            return false;
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
                }, Constants.Replication.DocumentReplicationConfiguration);
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
                }, Constants.Replication.DocumentReplicationConfiguration);
                session.SaveChanges();
            }
        }


        protected static void SetupReplication(DocumentStore fromStore, params DocumentStore[] toStores)
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
                    Destinations = destinations
                }, Constants.Replication.DocumentReplicationConfiguration);
                session.SaveChanges();
            }
        }		
    }
}
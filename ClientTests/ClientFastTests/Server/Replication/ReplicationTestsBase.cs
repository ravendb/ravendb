using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.NewClient.Client.Document;

namespace NewClientTests.NewClient.Server.Replication
{
    public class ReplicationTestsBase : RavenTestBase
    {
        
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
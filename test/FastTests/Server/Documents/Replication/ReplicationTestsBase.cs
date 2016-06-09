using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationTestsBase : RavenTestBase
    {
        protected void WaitForReplicationBetween(DocumentStore storeFrom, DocumentStore storeTo, int timeout)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                
            }
        }


        protected T WaitForDocumentToReplicate<T>(DocumentStore store, string id, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<T>(id);
                    if (doc != null)
                        return doc;
                }
                Thread.Sleep(25);
            }

            return default(T);
        }

        protected static void SetupReplication(string targetDbName, DocumentStore fromStore, DocumentStore toStore)
        {
            using (var session = fromStore.OpenSession())
            {
                session.Store(new ReplicationDocument
                {
                    Destinations = new List<ReplicationDestination>
                    {
                        new ReplicationDestination
                        {
                            Database = targetDbName,
                            Url = toStore.Url
                        }
                    }
                }, Constants.Replication.DocumentReplicationConfiguration);
                session.SaveChanges();
            }
        }
    }
}
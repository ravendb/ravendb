using System;
using System.Diagnostics;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Sparrow.Logging;
using Raven.Server.Utils;
using Voron.Exceptions;
using Xunit.Sdk;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MiscUtils.DisableLongTimespan = true;
            LoggingSource.Instance.SetupLogMode(LogMode.Information, @"c:\work\debug\ravendb");

            
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var a = new FastTests.Server.Replication.DisableDatabasePropagationInRaftCluster())
                {
                    a.DisableDatabaseToggleOperation_should_propagate_through_raft_cluster().Wait();
                }
            }
        }

        public static IDisposable Lock(IDocumentSession session, string docToLock)
        {
            var doc = session.Load<object>(docToLock);
            if (doc == null)
                throw new DocumentDoesNotExistException("The document " + docToLock + " does not exists and cannot be locked");

            var metadata = session.Advanced.GetMetadataFor(doc);


            if (metadata.GetBoolean("Pessimistic-Locked") &&
                // the document is locked and the lock is still value
                (DateTime.UtcNow <= new DateTime(metadata.GetNumber("Pessimistic-Lock-Timeout"))))
            {
                throw new ConcurrencyException("Document " + docToLock + " is locked using pessimistic");
            }
            
            metadata["Pessimistic-Locked"] = true;
            metadata["Pessimistic-Lock-Timeout"] = DateTime.UtcNow.AddSeconds(15).Ticks;

            
            // will throw if someone else took the look in the meantime
            session.Advanced.UseOptimisticConcurrency = true;
            session.SaveChanges();

            return new DisposableAction(() =>
            {
                metadata.Remove("Pessimistic-Locked");
                metadata.Remove("Pessimistic-Lock-Timeout");

                Debug.Assert(session.Advanced.UseOptimisticConcurrency);
                session.SaveChanges();
            });
        }
    }
}

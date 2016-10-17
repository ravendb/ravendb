// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3970.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3970 : ReplicationBase
    {
        [Fact]
        public void all_document_deletions_are_replicated_when_running_in_parallel_and_with_dtc()
        {
            using (var master = CreateStore(requestedStorageType: "esent"))
            using (var slave = CreateStore(requestedStorageType: "esent"))
            {
                SetupReplication(master.DatabaseCommands, slave);

                var testDocuments = new List<string>();
                var count = 0;

                using (var session = master.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

                    for (var i = 0; i < 1000; i++)
                    {
                        var id = "testDocuments/" + count++;

                        session.Store(new TestDocument
                        {
                            Name = "Document " + i,
                            EffectiveDate = DateTimeOffset.UtcNow
                        }, id);

                        testDocuments.Add(id);
                    }
                    session.SaveChanges();
                }

                WaitForDocument(slave.DatabaseCommands, testDocuments.Last());

                Parallel.ForEach(testDocuments, new ParallelOptions { MaxDegreeOfParallelism = 10 },
                testDocumentId =>
                {
                    using (var scope = new TransactionScope(TransactionScopeOption.Required))
                    {
                        using (var session = master.OpenSession())
                        {
                            var testDocument = session.Load<TestDocument>(testDocumentId);
                            session.Delete(testDocument);
                            session.SaveChanges();
                        }
                        scope.Complete();
                    }
                });

                var expectedCountOfDocs = master.DatabaseCommands.GetStatistics().CountOfDocuments;

                for (int i = 0; i < RetriesCount * 50; i++)
                {
                    if (slave.DatabaseCommands.GetStatistics().CountOfDocuments <= expectedCountOfDocs)
                        break;
                    Thread.Sleep(50);
                }

                Assert.Equal(expectedCountOfDocs, slave.DatabaseCommands.GetStatistics().CountOfDocuments);
            }
        }

        public class TestDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public DateTimeOffset EffectiveDate { get; set; }
        }
    }
}
// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4852.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4852 : ReplicationBase
    {
        [Theory]
        [InlineData(5000, "01000000-0000-0001-0000-000000001390")]
        [InlineData(10000, "01000000-0000-0001-0000-000000002719")]
        [InlineData(30000, "01000000-0000-0001-0000-00000000753A")]
        public void sources_document_will_be_replicated_in_etl(int count, string etag)
        {
            using (var master = CreateStore())
            using (var slave = CreateStore())
            {
                using (var bulk = master.BulkInsert())
                {
                    for (var i = 0; i < count; i++)
                    {
                        bulk.Store(new User { Name = "Grisha" });
                    }
                }

                Assert.NotNull(master.DatabaseCommands.Get($"users/{count}"));

                RunReplication(master, slave, specifiedCollections: new Dictionary<string, string>
                {
                    {
                        "Users", @"return null;"
                    }
                });

                var masterDatabaseId = master.DatabaseCommands.GetStatistics().DatabaseId;
                var id = $"Raven/Replication/Sources/{masterDatabaseId}";
                var requestedEtag = Etag.Parse(etag);
                SpinWait.SpinUntil(() =>
                {
                    using (var session = slave.OpenSession())
                    {
                        var sourcesDoc = session.Load<SourcesDocument>(id);
                        if (sourcesDoc == null ||
                            sourcesDoc.LastDocumentEtag.CompareTo(requestedEtag) < 0)
                        {
                            Thread.Sleep(100);
                            return false;
                        }

                        return true;
                    }
                }, 30000);

                using (var session = slave.OpenSession())
                {
                    var sourcesDoc = session.Load<SourcesDocument>(id);
                    Assert.NotNull(sourcesDoc);
                    Console.WriteLine(sourcesDoc.LastDocumentEtag);
                    Assert.True(sourcesDoc.LastDocumentEtag.CompareTo(requestedEtag) >= 0);
                }
            }
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class SourcesDocument
        {
            public Etag LastDocumentEtag { get; set; }
        }
    }
}

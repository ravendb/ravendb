// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1829.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1829 : ReplicationBase
    {
        public class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        [Fact]
        public async Task StreamQueryShouldHandleFailover()
        {
            var index = new RavenDocumentsByEntityName();

            using (var store1 = CreateStore(configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries))
            using (var store2 = CreateStore())
            {
                TellFirstInstanceToReplicateToSecondInstance();

                var replicationInformerForDatabase = store1.GetReplicationInformerForDatabase(store1.DefaultDatabase);
                await replicationInformerForDatabase.UpdateReplicationInformationIfNeeded((ServerClient)store1.DatabaseCommands);

                var people = InitializeData(store1);
                var lastPersonId = people.Last().Id;

                WaitForIndexing(store1);
                WaitForReplication(store2, lastPersonId);
                WaitForIndexing(store2);

                var count = 0;
                QueryHeaderInformation queryHeaderInfo;
                var enumerator = store1.DatabaseCommands.StreamQuery(index.IndexName, new IndexQuery(), out queryHeaderInfo);
                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(10, count);

                count = 0;
                enumerator = store2.DatabaseCommands.StreamQuery(index.IndexName, new IndexQuery(), out queryHeaderInfo);
                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(10, count);

                StopDatabase(0);

                count = 0;

                var failed = false;

                replicationInformerForDatabase.FailoverStatusChanged += (sender, args) => failed = true;
                enumerator = store1.DatabaseCommands.StreamQuery(index.IndexName, new IndexQuery(), out queryHeaderInfo);
                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(10, count);
                Assert.True(failed);
            }
        }

        [Fact]
        public async Task StreamDocsWithStartsWithShouldHandleFailover()
        {
            using (var store1 = CreateStore(configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries))
            using (var store2 = CreateStore())
            {
                TellFirstInstanceToReplicateToSecondInstance();

                var replicationInformerForDatabase = store1.GetReplicationInformerForDatabase(store1.DefaultDatabase);
                await replicationInformerForDatabase.UpdateReplicationInformationIfNeeded((ServerClient)store1.DatabaseCommands);

                var people = InitializeData(store1);
                var lastPersonId = people.Last().Id;

                WaitForIndexing(store1);
                WaitForReplication(store2, lastPersonId);
                WaitForIndexing(store2);

                var count = 0;
                var enumerator = store1.DatabaseCommands.StreamDocs(null, "people/");
                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(10, count);

                count = 0;
                enumerator = store2.DatabaseCommands.StreamDocs(null, "people/");
                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(10, count);

                StopDatabase(0);

                count = 0;

                var failed = false;

                replicationInformerForDatabase.FailoverStatusChanged += (sender, args) => failed = true;
                enumerator = store1.DatabaseCommands.StreamDocs(null, "people/");
                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(10, count);
                Assert.True(failed);
            }
        }

        [Fact]
        public async Task StreamDocsFromEtagShouldNotHandleFailover()
        {
            using (var store1 = CreateStore(configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries))
            using (var store2 = CreateStore())
            {
                TellFirstInstanceToReplicateToSecondInstance();

                var replicationInformerForDatabase = store1.GetReplicationInformerForDatabase(store1.DefaultDatabase);
                await replicationInformerForDatabase.UpdateReplicationInformationIfNeeded((ServerClient)store1.DatabaseCommands);

                var people = InitializeData(store1);
                var lastPersonId = people.Last().Id;
                var firstPersonId = people.First().Id;

                WaitForIndexing(store1);
                WaitForReplication(store2, lastPersonId);
                WaitForIndexing(store2);

                var startEtag1 = EtagUtil.Increment(store1.DatabaseCommands.Get(firstPersonId).Etag, -1);
                var startEtag2 = EtagUtil.Increment(store2.DatabaseCommands.Get(firstPersonId).Etag, -1);

                var count = 0;
                var enumerator = store1.DatabaseCommands.StreamDocs(startEtag1);
                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.True(count > 0);

                count = 0;
                enumerator = store2.DatabaseCommands.StreamDocs(startEtag2);
                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.True(count > 0);

                StopDatabase(0);

                var e = Assert.Throws<AggregateException>(() => store1.DatabaseCommands.StreamDocs(startEtag1));
                var requestException = e.InnerException as HttpRequestException;

                Assert.NotNull(requestException);
            }
        }

        private static IList<Person> InitializeData(IDocumentStore store)
        {
            var results = new List<Person>();
            for (var i = 0; i < 10; i++)
            {
                results.Add(new Person { Name = "Name" + i });
            }

            using (var session = store.OpenSession())
            {
                foreach (var person in results)
                {
                    session.Store(person);
                }

                session.SaveChanges();
            }

            return results;
        }
    }
}
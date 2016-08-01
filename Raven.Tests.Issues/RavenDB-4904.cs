// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4904 : RavenTest
    {
        private const string IndexName = "testIndex";

        [Fact]
        public void can_create_side_by_side_index_to_replace_index_with_errors()
        {
            using (var store = NewRemoteDocumentStore(true))
            {

                store.DatabaseCommands.Put("companies/1", null, new RavenJObject { { "Name", "HR" } }, new RavenJObject());
                Assert.Equal(0, store.DatabaseCommands.GetStatistics().Errors.Length);

                store.DatabaseCommands.PutIndex(IndexName, new IndexDefinition { Map = "from doc in docs let x = 0 select new { Total = 3/x };" });
                WaitForIndexing(store);
                Assert.Equal(1, store.DatabaseCommands.GetStatistics().Errors.Length);

                store.DatabaseCommands.PutSideBySideIndexes(new[]
                {
                    new IndexToAdd
                    {
                        Name = IndexName,
                        Definition = new IndexDefinition { Map = "from doc in docs select new { Total = 3/1 };" }
                    }
                });

                SpinWait.SpinUntil(() => store.DatabaseCommands.GetStatistics().Indexes.Length == 2);
                Assert.Equal(0, store.DatabaseCommands.GetStatistics().Errors.Count(x => x.IndexName == IndexName));
                Assert.Equal(0, store.DatabaseCommands.GetStatistics().Errors.Count(x => x.IndexName != IndexName));
            }
        }

        [Fact]
        public void can_create_side_by_side_index_with_errors_to_replace_index_with_errors()
        {
            using (var store = NewRemoteDocumentStore(true))
            {
                store.DatabaseCommands.Put("companies/1", null, new RavenJObject { { "Name", "HR" } }, new RavenJObject());
                Assert.Equal(0, store.DatabaseCommands.GetStatistics().Errors.Length);

                store.DatabaseCommands.PutIndex(IndexName, new IndexDefinition { Map = "from doc in docs let x = 0 select new { Total = 3/x };" });
                WaitForIndexing(store);
                Assert.Equal(1, store.DatabaseCommands.GetStatistics().Errors.Length);

                store.DatabaseCommands.PutSideBySideIndexes(new[]
                {
                    new IndexToAdd
                    {
                        Name = IndexName,
                        Definition = new IndexDefinition { Map = "from doc in docs let x = 0 select new { Total = 3/x };" }
                    }
                });

                SpinWait.SpinUntil(() => store.DatabaseCommands.GetStatistics().Indexes.Length == 2);
                Assert.Equal(1, store.DatabaseCommands.GetStatistics().Errors.Count(x => x.IndexName == IndexName));
                Assert.Equal(0, store.DatabaseCommands.GetStatistics().Errors.Count(x => x.IndexName != IndexName));
            }
        }

        [Fact]
        public void can_create_side_by_side_index_with_errors_to_replace_index()
        {
            using (var store = NewRemoteDocumentStore(true))
            {
                store.DatabaseCommands.Put("companies/1", null, new RavenJObject { { "Name", "HR" } }, new RavenJObject());
                Assert.Equal(0, store.DatabaseCommands.GetStatistics().Errors.Length);

                store.DatabaseCommands.PutIndex(IndexName, new IndexDefinition { Map = "from doc in docs select new { Total = 3/1 };" });
                WaitForIndexing(store);
                Assert.Equal(0, store.DatabaseCommands.GetStatistics().Errors.Length);

                store.DatabaseCommands.PutSideBySideIndexes(new[]
                {
                    new IndexToAdd
                    {
                        Name = IndexName,
                        Definition = new IndexDefinition { Map = "from doc in docs let x = 0 select new { Total = 3/x };" }
                    }
                });

                SpinWait.SpinUntil(() => store.DatabaseCommands.GetStatistics().Indexes.Length == 2);
                Assert.Equal(1, store.DatabaseCommands.GetStatistics().Errors.Count(x => x.IndexName == IndexName));
                Assert.Equal(0, store.DatabaseCommands.GetStatistics().Errors.Count(x => x.IndexName != IndexName));
            }
        }

        public class Company
        {
            public string Name { get; set; }
        }
    }
}

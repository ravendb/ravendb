using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14323 : RavenTestBase
    {
        public RavenDB_14323(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Reference { get; set; }
        }

        private class Parent
        {
            public string Id { get; set; }

            public string NumericId { get; set; }
        }

        [Fact]
        public void Index_State_Error()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                store.ExecuteIndex(index);

                string parentDocumentId;
                using (var session = store.OpenSession())
                {
                    var parent = new Parent
                    {
                        NumericId = "a"
                    };

                    session.Store(parent);
                    parentDocumentId = parent.Id;

                    session.Store(new User
                    {
                        Reference = parentDocumentId
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));

                Assert.True(indexStats.IsInvalidIndex);
                Assert.Equal(IndexState.Error, indexStats.State);
            }
        }

        [Fact]
        public void Index_State_Error_After_Change()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                store.ExecuteIndex(index);

                string parentDocumentId;
                using (var session = store.OpenSession())
                {
                    var parent = new Parent
                    {
                        NumericId = "123"
                    };

                    session.Store(parent);
                    parentDocumentId = parent.Id;

                    session.Store(new User
                    {
                        Reference = parentDocumentId
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var parent = session.Load<Parent>(parentDocumentId);
                    parent.NumericId = "a";
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));

                Assert.True(indexStats.IsInvalidIndex);
                Assert.Equal(IndexState.Error, indexStats.State);
            }
        }

        private class Index : AbstractIndexCreationTask<User>
        {
            public Index()
            {
                Map = users => from user in users
                    let parent = LoadDocument<Parent>(user.Reference)
                    select new
                    {
                        Number = int.Parse(parent.NumericId)
                    };
            }
        }
    }
}

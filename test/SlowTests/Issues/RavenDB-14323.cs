using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        [InlineData(20)]
        public void Index_State_Error(int numberOfReferencedDocuments)
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < numberOfReferencedDocuments; i++)
                    {
                        var parent = new Parent
                        {
                            NumericId = "a"
                        };

                        session.Store(parent);

                        session.Store(new User
                        {
                            Reference = parent.Id
                        });
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                Assert.True(SpinWait.SpinUntil(() =>
                {
                    var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
                    return stats.IsInvalidIndex;
                }, TimeSpan.FromSeconds(10))); //precaution

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));

                Assert.True(indexStats.IsInvalidIndex);
                Assert.Equal(IndexState.Error, indexStats.State);

                Assert.Equal(numberOfReferencedDocuments, indexStats.MapAttempts);
                Assert.Equal(0, indexStats.MapSuccesses);
                Assert.Equal(numberOfReferencedDocuments, indexStats.MapErrors);

                Assert.Equal(0, indexStats.MapReferenceAttempts);
                Assert.Equal(0, indexStats.MapReferenceSuccesses);
                Assert.Equal(0, indexStats.MapReferenceErrors);
            }
        }

        [Theory]
        [InlineData(5)]
        [InlineData(10)]
        [InlineData(20)]
        public void Index_State_Error_After_Change(int numberOfReferencedDocuments)
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                store.ExecuteIndex(index);

                var referencedDocuments = new List<string>();
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < numberOfReferencedDocuments; i++)
                    {
                        var parent = new Parent
                        {
                            NumericId = "123"
                        };

                        session.Store(parent);
                        var parentDocumentId = parent.Id;

                        session.Store(new User
                        {
                            Reference = parentDocumentId
                        });

                        referencedDocuments.Add(parentDocumentId);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    foreach (var referencedDocument in referencedDocuments)
                    {
                        var parent = session.Load<Parent>(referencedDocument);
                        parent.NumericId = "a";
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                Assert.True(SpinWait.SpinUntil(() =>
                {
                    var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
                    return stats.IsInvalidIndex;
                }, TimeSpan.FromSeconds(10))); //precaution

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));

                Assert.True(indexStats.IsInvalidIndex);
                Assert.Equal(IndexState.Error, indexStats.State);

                Assert.Equal(numberOfReferencedDocuments, indexStats.MapAttempts);
                Assert.Equal(numberOfReferencedDocuments, indexStats.MapSuccesses);
                Assert.Equal(0, indexStats.MapErrors);
                Assert.Equal(numberOfReferencedDocuments, indexStats.MapReferenceAttempts);
                Assert.Equal(0, indexStats.MapReferenceSuccesses);
                Assert.Equal(numberOfReferencedDocuments, indexStats.MapReferenceErrors);
            }
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

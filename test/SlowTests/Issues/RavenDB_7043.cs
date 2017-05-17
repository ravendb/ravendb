using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7043 : RavenTestBase
    {
        [Fact]
        public void Should_mark_index_as_errored_and_throw_on_querying_even_if_its_small()
        {
            using (var store = GetDocumentStore())
            {
                var failingIndex = new Failing_index();
                failingIndex.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User());
                    }

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                
                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<RavenException>(() =>session.Query<User, Failing_index>().ToList());
                    
                    Assert.Contains("Index \'Failing/index (3)\' is marked as errored. Index Failing/index (3) is invalid, out of 10 map attempts, 10 has failed. Error rate of 100% exceeds allowed 15% error rate", ex.Message);
                }

                var indexStats = store.Admin.Send(new GetIndexStatisticsOperation(failingIndex.IndexName));

                Assert.True(indexStats.IsInvalidIndex);
                Assert.Equal(IndexState.Error, indexStats.State);
            }
        }

        private class Failing_index : AbstractIndexCreationTask<User>
        {
            public Failing_index()
            {
                Map = users => from u in users
                    select new { a = 10 / (u.Age - u.Age) };
            }
        }
    }
}
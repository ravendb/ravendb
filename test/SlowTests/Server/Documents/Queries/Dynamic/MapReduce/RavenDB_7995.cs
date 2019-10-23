using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Queries.Dynamic.MapReduce
{
    public class RavenDB_7995 : RavenTestBase
    {
        public RavenDB_7995(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Cannot_filter_if_field_isnt_aggregation_nor_part_of_group_by_key()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<InvalidQueryException>(() => session.Advanced.DocumentQuery<User>().GroupBy("Country").SelectCount().WhereEquals("City", "London").ToList());

                    Assert.Contains("Field 'City' isn't neither an aggregation operation nor part of the group by key", ex.Message);
                }
            }
        }

        [Fact]
        public void Queries_specifying_different_ordering_in_group_by_should_be_handled_by_the_same_index()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Age = 31,
                        Name = "Arek"
                    });

                    session.Store(new User
                    {
                        Age = 31,
                        Name = "Arek"
                    });

                    session.SaveChanges();

                    QueryStatistics stats1;
                    var results = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Statistics(out stats1).GroupBy(x => new
                    {
                        x.Age,
                        x.Name
                    }).Select(x => new
                    {
                        x.Key,
                        Count = x.Count(),
                    }).ToList();

                    Assert.Equal(2, results[0].Count);

                    QueryStatistics stats2;
                    var results2 = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Statistics(out stats2).GroupBy(x => new
                    {
                        x.Name,
                        x.Age
                    }).Select(x => new
                    {
                        x.Key,
                        Count = x.Count(),
                    }).ToList();

                    Assert.Equal(2, results2[0].Count);

                    Assert.Equal(stats1.IndexName, stats2.IndexName);

                    var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                    Assert.Equal(1, indexDefinitions.Length);
                }
            }
        }
    }
}

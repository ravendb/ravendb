using System.Linq;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic.MapReduce
{
    public class RavenDB_7044 : RavenTestBase
    {
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
    }
}

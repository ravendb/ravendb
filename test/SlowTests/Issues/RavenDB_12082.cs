using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12082 : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask<Company>
        {
            public Index1()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };

                Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "55";
            }
        }

        [Fact]
        public void CanConfigureIndexViaAbstractIndexCreationTask()
        {
            var definition = new Index1().CreateIndexDefinition();

            Assert.Equal("55", definition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)]);
        }
    }
}

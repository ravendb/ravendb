using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15935 : RavenTestBase
    {
        public RavenDB_15935(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ExecutingIndexOnEmptyDefaultDatabase_ShouldThrow()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { Server.WebUrl }
            })
            {
                store.Initialize();

                var e = Assert.Throws<InvalidOperationException>(() => new Companies_ByName().Execute(store));

                Assert.Contains("Cannot determine database to operate on", e.Message);
            }
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };
            }
        }
    }
}

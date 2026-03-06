using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Tests.Infrastructure;

namespace SlowTests.MailingList
{
    public class Joel : RavenTestBase
    {
        public Joel(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        private class Index : AbstractIndexCreationTask<Item, Index.Result>
        {
            public class Result
            {
                public object Query { get; set; }
            }

            public Index()
            {
                Map = items =>
                      from item in items
                      select new Result
                      {
                          Query = new object[] { item.Age, item.Name }
                      };
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void CanCreateIndexWithExplicitType()
        {
            using (var store = GetDocumentStore())
            {
                new Index().Execute(store);
                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("Index"));
                RavenTestHelper.AssertEqualRespectingNewLines(@"docs.Items.Select(item => new {
    Query = new object[] {
        item.Age,
        item.Name
    }
})", indexDefinition.Maps.First());
            }
        }
    }
}

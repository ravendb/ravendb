using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class CanIndexNestedObjects : RavenTestBase
    {
        public CanIndexNestedObjects(ITestOutputHelper output) : base(output)
        {
        }

        private class NestedObject
        {
            public string Name { get; set; }
            public int Quantity { get; set; }
        }

        private class ContainerObject
        {
            public string ContainerName { get; set; }
            public IEnumerable<NestedObject> Items { get; set; }
        }

        private class IndexEntry
        {
            public string ContainerName { get; set; }
            public string Name { get; set; }
            public int Quantity { get; set; }
        }

        private class NestedObjectIndex : AbstractIndexCreationTask<ContainerObject, IndexEntry>
        {
            public NestedObjectIndex()
            {
                Map = docs => from doc in docs
                              from item in doc.Items
                              select new
                              {
                                  doc.ContainerName,
                                  item.Name,
                                  item.Quantity
                              };
                Store(x => x.Name, FieldStorage.Yes);
                Store(x => x.ContainerName, FieldStorage.Yes);
                Store(x => x.Quantity, FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SimpleInsertAndRead(Options options)
        {
            string expectedContainerName = "someContainer123098";
            string expectedItemName = "someItem456";
            int expectedQuantity = 123;

            using (var store = GetDocumentStore(options))
            {
                new NestedObjectIndex().Execute(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new ContainerObject()
                    {
                        ContainerName = expectedContainerName,
                        Items = new[]
                        {
                            new NestedObject()
                            {
                                Name = expectedItemName,
                                Quantity = expectedQuantity
                            },
                            new NestedObject()
                            {
                                Name = "something Else",
                                Quantity = 345
                            }
                        }
                    });

                    s.SaveChanges();
                }

                //  the index has two objects
                using (var s = store.OpenSession())
                {
                    var result = s.Query<ContainerObject, NestedObjectIndex>()
                        .Customize(q => q.WaitForNonStaleResults())
                        .Count();

                    Assert.Equal(2, result);
                }

                //  and the index can be queried
                using (var s = store.OpenSession())
                {
                    var result = s
                        .Query<ContainerObject, NestedObjectIndex>()
                        .Customize(q => q.WaitForNonStaleResults())
                        .ProjectInto<IndexEntry>()
                        .Single(o => o.Name == expectedItemName);

                    Assert.Equal(expectedContainerName, result.ContainerName);
                    Assert.Equal(expectedItemName, result.Name);
                    Assert.Equal(expectedQuantity, result.Quantity);
                }
            }
        }
    }
}

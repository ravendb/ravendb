using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
    public class CanIndexNestedObjects : LocalClientTest
    {
        public class NestedObject
        {
            public string Name;
            public int Quantity;
        }

        public class ContainerObject
        {
            public string ContainerName;
            public IEnumerable<NestedObject> Items;
        }

        public class IndexEntry
        {
            public string ContainerName;
            public string Name;
            public int Quantity;
        }

        public class NestedObjectIndex : AbstractIndexCreationTask<ContainerObject, IndexEntry>
        {
            public override Abstractions.Indexing.IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinitionBuilder<ContainerObject, IndexEntry>()
                {
                    Map = docs => from doc in docs
                                  from item in doc.Items
                                  select new
                                  {
                                      ContainerName = doc.ContainerName,
                                      Name = item.Name,
                                      Quantity = item.Quantity
                                  }

                }.ToIndexDefinition(Conventions);
            }
        }

        [Fact]
        public void SimpleInsertAndRead()
        {
            string expectedContainerName = "someContainer123098";
            string expectedItemName = "someItem456";
            int expectedQuantity = 123;

            using (var store = NewDocumentStore())
            {
                IndexCreation.CreateIndexes(typeof(NestedObjectIndex).Assembly, store);
                

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
                    var result = s.Query<IndexEntry, NestedObjectIndex>()
                        .Customize(q => q.WaitForNonStaleResultsAsOfNow())
                        .ToArray();

                    Assert.Equal(2, result.Count());
                }

                //  and the index can be queried
                using (var s = store.OpenSession())
                {
                    var result = s.Query<IndexEntry, NestedObjectIndex>()
                        .Customize(q => q.WaitForNonStaleResultsAsOfNow())
                        .Where(o => o.Name == expectedItemName)
                        .Single();

                    Assert.Equal(expectedContainerName, result.ContainerName);
                    Assert.Equal(expectedItemName, result.Name);
                    Assert.Equal(expectedQuantity, result.Quantity);
                }
            }
        }
    }
}

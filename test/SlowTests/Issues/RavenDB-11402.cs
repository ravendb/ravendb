using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11402 : RavenTestBase
    {
        public RavenDB_11402(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanWaitForIndex()
        {
            using (var documentStore = GetDocumentStore())
            {
                var index = new TestIndex();
                documentStore.ExecuteIndex(index);
                documentStore.Initialize();

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Entity
                    {
                        Id = "Entity/1",
                        Test = "A",
                        Properties = new List<PropertyClass>
                    {
                        new PropertyClass
                        {
                            Property = "Property1"
                        }
                    }
                    });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<TestIndex.Result, TestIndex>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .ProjectInto<TestIndex.Result>()
                        .ToList();

                    var errors = Indexes.WaitForIndexingErrors(documentStore, errorsShouldExists: false);
                    Assert.Null(errors);
                    Assert.Equal(1, results.Count);
                }
            }
        }
    }

    public class Entity
    {
        public string Id { get; set; }
        public string Test { get; set; }
        public List<PropertyClass> Properties { get; set; }
    }

    public class PropertyClass
    {
        public string Property { get; set; }
    }

    public class TestIndex : AbstractIndexCreationTask<Entity, TestIndex.Result>
    {
        public class Result
        {
            public string Test { get; set; }
            public List<PropertyClass> Properties { get; set; }
        }

        public TestIndex()
        {
            Map = entities => from entity in entities
                              let properties = entity.Test == "B" ?
                                entity.Properties.Take(1) :
                                new PropertyClass[0]
                              select new
                              {
                                  Test = entity.Test,
                                  Properites = from property in properties
                                               select new
                                               {
                                                   Property = property.Property + "Test"
                                               }
                              };

            StoreAllFields(FieldStorage.Yes);
        }
    }
}

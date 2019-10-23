using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_1379_Client_Lazy : RavenTestBase
    {
        public RavenDB_1379_Client_Lazy(ITestOutputHelper output) : base(output)
        {
        }

        public class SomeEntity
        {
            public string Id { get; set; }
        }

        [Fact]
        public void PagingWithoutFilters()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new SomeEntity { Id = "FooBar1" });
                    session.Store(new SomeEntity { Id = "BarFoo2" });
                    session.Store(new SomeEntity { Id = "FooBar3" });
                    session.Store(new SomeEntity { Id = "FooBar11" });
                    session.Store(new SomeEntity { Id = "FooBar12" });
                    session.Store(new SomeEntity { Id = "FooBar21" });
                    session.Store(new SomeEntity { Id = "FooBar5" });
                    session.Store(new SomeEntity { Id = "BarFoo7" });
                    session.Store(new SomeEntity { Id = "FooBar111" });
                    session.Store(new SomeEntity { Id = "BarFoo6" });
                    session.Store(new SomeEntity { Id = "FooBar6" });
                    session.Store(new SomeEntity { Id = "FooBar8" });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 0, 4, string.Empty)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(4, foundDocKeys.Count);
                    Assert.Contains("FooBar1", foundDocKeys);
                    Assert.Contains("FooBar11", foundDocKeys);
                    Assert.Contains("FooBar111", foundDocKeys);
                    Assert.Contains("FooBar12", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 4, 4, string.Empty)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(4, foundDocKeys.Count);
                    Assert.Contains("FooBar21", foundDocKeys);
                    Assert.Contains("FooBar3", foundDocKeys);
                    Assert.Contains("FooBar5", foundDocKeys);
                    Assert.Contains("FooBar6", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 8, 4, string.Empty)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar8", foundDocKeys);
                }
            }
        }


        [Fact]
        public void PagingWithoutFiltersWithPagingInformation()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new SomeEntity { Id = "FooBar1" });
                    session.Store(new SomeEntity { Id = "BarFoo2" });
                    session.Store(new SomeEntity { Id = "FooBar3" });
                    session.Store(new SomeEntity { Id = "FooBar11" });
                    session.Store(new SomeEntity { Id = "FooBar12" });
                    session.Store(new SomeEntity { Id = "FooBar21" });
                    session.Store(new SomeEntity { Id = "FooBar5" });
                    session.Store(new SomeEntity { Id = "BarFoo7" });
                    session.Store(new SomeEntity { Id = "FooBar111" });
                    session.Store(new SomeEntity { Id = "BarFoo6" });
                    session.Store(new SomeEntity { Id = "FooBar6" });
                    session.Store(new SomeEntity { Id = "FooBar8" });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 0, 4, string.Empty)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(4, foundDocKeys.Count);
                    Assert.Contains("FooBar1", foundDocKeys);
                    Assert.Contains("FooBar11", foundDocKeys);
                    Assert.Contains("FooBar111", foundDocKeys);
                    Assert.Contains("FooBar12", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 4, 4, string.Empty)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(4, foundDocKeys.Count);
                    Assert.Contains("FooBar21", foundDocKeys);
                    Assert.Contains("FooBar3", foundDocKeys);
                    Assert.Contains("FooBar5", foundDocKeys);
                    Assert.Contains("FooBar6", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 8, 4, string.Empty)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar8", foundDocKeys);
                }
            }
        }


        [Fact]
        public void PagingWithExcludesWithPagingInformation()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new SomeEntity { Id = "FooBar1" });
                    session.Store(new SomeEntity { Id = "BarFoo2" });
                    session.Store(new SomeEntity { Id = "FooBar3" });
                    session.Store(new SomeEntity { Id = "FooBar11" });
                    session.Store(new SomeEntity { Id = "FooBar12" });
                    session.Store(new SomeEntity { Id = "FooBar21" });
                    session.Store(new SomeEntity { Id = "FooBar5" });
                    session.Store(new SomeEntity { Id = "BarFoo7" });
                    session.Store(new SomeEntity { Id = "FooBar111" });
                    session.Store(new SomeEntity { Id = "BarFoo6" });
                    session.Store(new SomeEntity { Id = "FooBar6" });
                    session.Store(new SomeEntity { Id = "FooBar8" });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 0, 2, exclude: "1*")
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar21", foundDocKeys);
                    Assert.Contains("FooBar3", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 2, 2, exclude: "1*")
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar5", foundDocKeys);
                    Assert.Contains("FooBar6", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 4, 2, exclude: "1*")
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar8", foundDocKeys);
                }
            }
        }



        [Fact]
        public void PagingWithMatchesWithPagingInformation()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new SomeEntity { Id = "FooBar1" });
                    session.Store(new SomeEntity { Id = "BarFoo2" });
                    session.Store(new SomeEntity { Id = "FooBar3" });
                    session.Store(new SomeEntity { Id = "FooBar11" });
                    session.Store(new SomeEntity { Id = "FooBar12" });
                    session.Store(new SomeEntity { Id = "FooBar21" });
                    session.Store(new SomeEntity { Id = "FooBar5" });
                    session.Store(new SomeEntity { Id = "BarFoo7" });
                    session.Store(new SomeEntity { Id = "FooBar111" });
                    session.Store(new SomeEntity { Id = "BarFoo6" });
                    session.Store(new SomeEntity { Id = "FooBar6" });
                    session.Store(new SomeEntity { Id = "FooBar8" });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 0, 2)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar1", foundDocKeys);
                    Assert.Contains("FooBar11", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 2, 1)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar111", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 3, 10)
                        .Value
                        .ToList();

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Value.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar12", foundDocKeys);
                }
            }
        }
    }
}

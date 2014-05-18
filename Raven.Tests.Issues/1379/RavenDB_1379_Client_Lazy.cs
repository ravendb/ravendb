using System.Collections.Generic;
using System.Linq;

using Raven.Client;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1379_Client_Lazy : RavenTest
	{
        public class SomeEntity
        {
            public string Id { get; set; }
        }

	    [Theory]
        [PropertyData("Storages")]
	    public void PagingWithoutFilters(string requestedStorage)
	    {
	        using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
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

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

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

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

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

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

	                Assert.Equal(1, foundDocKeys.Count);
	                Assert.Contains("FooBar8", foundDocKeys);
	            }
	        }
	    }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithoutFilters_Remote(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
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

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

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

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

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

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar8", foundDocKeys);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithoutFiltersWithPagingInformation(string requestedStorage)
        {
            using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
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

                var pagingInformation = new RavenPagingInformation();

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 0, 4, string.Empty, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(4, pagingInformation.PageSize);
                    Assert.Equal(0, pagingInformation.Start);
                    Assert.Equal(4, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(4, foundDocKeys.Count);
                    Assert.Contains("FooBar1", foundDocKeys);
                    Assert.Contains("FooBar11", foundDocKeys);
                    Assert.Contains("FooBar111", foundDocKeys);
                    Assert.Contains("FooBar12", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 4, 4, string.Empty, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(4, pagingInformation.PageSize);
                    Assert.Equal(4, pagingInformation.Start);
                    Assert.Equal(8, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(4, foundDocKeys.Count);
                    Assert.Contains("FooBar21", foundDocKeys);
                    Assert.Contains("FooBar3", foundDocKeys);
                    Assert.Contains("FooBar5", foundDocKeys);
                    Assert.Contains("FooBar6", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 8, 4, string.Empty, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(4, pagingInformation.PageSize);
                    Assert.Equal(8, pagingInformation.Start);
                    Assert.Equal(8, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar8", foundDocKeys);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithoutFiltersWithPagingInformation_Remote(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
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

                var pagingInformation = new RavenPagingInformation();

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 0, 4, string.Empty, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(4, pagingInformation.PageSize);
                    Assert.Equal(0, pagingInformation.Start);
                    Assert.Equal(4, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(4, foundDocKeys.Count);
                    Assert.Contains("FooBar1", foundDocKeys);
                    Assert.Contains("FooBar11", foundDocKeys);
                    Assert.Contains("FooBar111", foundDocKeys);
                    Assert.Contains("FooBar12", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 4, 4, string.Empty, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(4, pagingInformation.PageSize);
                    Assert.Equal(4, pagingInformation.Start);
                    Assert.Equal(8, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(4, foundDocKeys.Count);
                    Assert.Contains("FooBar21", foundDocKeys);
                    Assert.Contains("FooBar3", foundDocKeys);
                    Assert.Contains("FooBar5", foundDocKeys);
                    Assert.Contains("FooBar6", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 8, 4, string.Empty, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(4, pagingInformation.PageSize);
                    Assert.Equal(8, pagingInformation.Start);
                    Assert.Equal(8, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar8", foundDocKeys);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithExcludesWithPagingInformation(string requestedStorage)
        {
            using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
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

                var pagingInformation = new RavenPagingInformation();

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 0, 2, pagingInformation: pagingInformation, exclude: "1*")
                        .Value
                        .ToList();

                    Assert.Equal(0, pagingInformation.Start);
                    Assert.Equal(2, pagingInformation.PageSize);
                    Assert.Equal(6, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar21", foundDocKeys);
                    Assert.Contains("FooBar3", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 2, 2, pagingInformation: pagingInformation, exclude: "1*")
                        .Value
                        .ToList();

                    Assert.Equal(2, pagingInformation.Start);
                    Assert.Equal(2, pagingInformation.PageSize);
                    Assert.Equal(8, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar5", foundDocKeys);
                    Assert.Contains("FooBar6", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 4, 2, pagingInformation: pagingInformation, exclude: "1*")
                        .Value
                        .ToList();

                    Assert.Equal(4, pagingInformation.Start);
                    Assert.Equal(2, pagingInformation.PageSize);
                    Assert.Equal(8, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar8", foundDocKeys);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithExcludesWithPagingInformation_Remote(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
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

                var pagingInformation = new RavenPagingInformation();

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 0, 2, pagingInformation: pagingInformation, exclude: "1*")
                        .Value
                        .ToList();

                    Assert.Equal(0, pagingInformation.Start);
                    Assert.Equal(2, pagingInformation.PageSize);
                    Assert.Equal(6, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar21", foundDocKeys);
                    Assert.Contains("FooBar3", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 2, 2, pagingInformation: pagingInformation, exclude: "1*")
                        .Value
                        .ToList();

                    Assert.Equal(2, pagingInformation.Start);
                    Assert.Equal(2, pagingInformation.PageSize);
                    Assert.Equal(8, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar5", foundDocKeys);
                    Assert.Contains("FooBar6", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", string.Empty, 4, 2, pagingInformation: pagingInformation, exclude: "1*")
                        .Value
                        .ToList();

                    Assert.Equal(4, pagingInformation.Start);
                    Assert.Equal(2, pagingInformation.PageSize);
                    Assert.Equal(8, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar8", foundDocKeys);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithMatchesWithPagingInformation(string requestedStorage)
        {
            using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
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

                var pagingInformation = new RavenPagingInformation();

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 0, 2, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(0, pagingInformation.Start);
                    Assert.Equal(2, pagingInformation.PageSize);
                    Assert.Equal(2, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar1", foundDocKeys);
                    Assert.Contains("FooBar11", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 2, 1, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(2, pagingInformation.Start);
                    Assert.Equal(1, pagingInformation.PageSize);
                    Assert.Equal(3, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar111", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 3, 10, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(3, pagingInformation.Start);
                    Assert.Equal(10, pagingInformation.PageSize);
                    Assert.Equal(3, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar12", foundDocKeys);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithMatchesWithPagingInformation_Remote(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
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

                var pagingInformation = new RavenPagingInformation();

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 0, 2, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(0, pagingInformation.Start);
                    Assert.Equal(2, pagingInformation.PageSize);
                    Assert.Equal(2, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(2, foundDocKeys.Count);
                    Assert.Contains("FooBar1", foundDocKeys);
                    Assert.Contains("FooBar11", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 2, 1, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(2, pagingInformation.Start);
                    Assert.Equal(1, pagingInformation.PageSize);
                    Assert.Equal(3, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar111", foundDocKeys);
                }

                using (var session = documentStore.OpenSession())
                {
                    var fetchedDocuments = session.Advanced.Lazily
                        .LoadStartingWith<SomeEntity>("FooBar", "1*", 3, 10, pagingInformation: pagingInformation)
                        .Value
                        .ToList();

                    Assert.Equal(3, pagingInformation.Start);
                    Assert.Equal(10, pagingInformation.PageSize);
                    Assert.Equal(3, pagingInformation.NextPageStart);

                    var foundDocKeys = fetchedDocuments.Select(doc => doc.Id).ToList();

                    Assert.Equal(1, foundDocKeys.Count);
                    Assert.Contains("FooBar12", foundDocKeys);
                }
            }
        }
	}
}

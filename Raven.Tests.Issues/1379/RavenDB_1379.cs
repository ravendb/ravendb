using System.Linq;
using System.Threading;

using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1379 : RavenTest
	{
	    [Theory]
        [PropertyData("Storages")]
	    public void PagingWithoutFilters(string requestedStorage)
	    {
	        using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
	        {
	            documentStore.DocumentDatabase.Documents.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Documents.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

		        int nextPageStart = 0;
		        var fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, 0, 4, CancellationToken.None, ref nextPageStart)
                    .ToList();

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                                   .Select(doc => doc.Value<string>("@id"))
                                                   .ToList();

                Assert.Equal(4, foundDocKeys.Count);
				Assert.Equal(4, nextPageStart);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);
                Assert.Contains("FooBar111", foundDocKeys);
                Assert.Contains("FooBar12", foundDocKeys);

		        nextPageStart = 0;
                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, 4, 4, CancellationToken.None, ref nextPageStart)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(4, foundDocKeys.Count);
				Assert.Equal(8, nextPageStart);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

				nextPageStart = 0;
                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, 8, 4, CancellationToken.None, ref nextPageStart)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
				Assert.Equal(8, nextPageStart);
                Assert.Contains("FooBar8", foundDocKeys);
	        }
	    }

		[Theory]
        [PropertyData("Storages")]
		public void PagingWithoutFiltersWithNextPageStart(string requestedStorage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Documents.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

				int nextPageStart = 0;
				var fetchedDocuments = documentStore
					.DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, nextPageStart, 4, CancellationToken.None, ref nextPageStart)
					.ToList();

				var foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
												   .Select(doc => doc.Value<string>("@id"))
												   .ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Equal(4, nextPageStart);
				Assert.Contains("FooBar1", foundDocKeys);
				Assert.Contains("FooBar11", foundDocKeys);
				Assert.Contains("FooBar111", foundDocKeys);
				Assert.Contains("FooBar12", foundDocKeys);

				fetchedDocuments = documentStore
					.DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, nextPageStart, 4, CancellationToken.None, ref nextPageStart)
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
											   .Select(doc => doc.Value<string>("@id"))
											   .ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Equal(8, nextPageStart);
				Assert.Contains("FooBar21", foundDocKeys);
				Assert.Contains("FooBar3", foundDocKeys);
				Assert.Contains("FooBar5", foundDocKeys);
				Assert.Contains("FooBar6", foundDocKeys);

				fetchedDocuments = documentStore
					.DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, nextPageStart, 4, CancellationToken.None, ref nextPageStart)
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
											   .Select(doc => doc.Value<string>("@id"))
											   .ToList();

				Assert.Equal(1, foundDocKeys.Count);
				Assert.Equal(8, nextPageStart);
				Assert.Contains("FooBar8", foundDocKeys);
			}
		}

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithExcludes(string requestedStorage)
        {
            using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DocumentDatabase.Documents.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

	            int nextPageStart = 0;
	            var fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 0, 2, CancellationToken.None, ref nextPageStart)
                    .ToList();

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                                   .Select(doc => doc.Value<string>("@id"))
                                                   .ToList();

                Assert.Equal(2, foundDocKeys.Count);
				Assert.Equal(6, nextPageStart);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);

	            nextPageStart = 0;
                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 2, 2, CancellationToken.None, ref nextPageStart)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(2, foundDocKeys.Count);
				Assert.Equal(8, nextPageStart);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

				nextPageStart = 0;
                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 4, 2, CancellationToken.None, ref nextPageStart)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
				Assert.Equal(4, nextPageStart);
                Assert.Contains("FooBar8", foundDocKeys);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithMatches(string requestedStorage)
        {
            using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DocumentDatabase.Documents.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Documents.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

	            int nextPageStart = 0;
	            var fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 0, 2, CancellationToken.None, ref nextPageStart)
                    .ToList();

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                                   .Select(doc => doc.Value<string>("@id"))
                                                   .ToList();

                Assert.Equal(2, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);

				nextPageStart = 0;
                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 2, 1, CancellationToken.None, ref nextPageStart)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar111", foundDocKeys);

				nextPageStart = 0;
                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .Documents
                    .GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 3, 10, CancellationToken.None, ref nextPageStart)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar12", foundDocKeys);
            }
        }

	    [Theory]
        [PropertyData("Storages")]
		public void GetDocumentsWithIdStartingWith_Should_Not_Count_Excluded_Docs(string requestedStorage)
		{
			using(var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Documents.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

				int nextPageStart = 0;
                var fetchedDocuments = documentStore.DocumentDatabase.Documents.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 0, 4, CancellationToken.None, ref nextPageStart);

				var documentsList = fetchedDocuments.ToList();
				var foundDocKeys = documentsList.Select(doc => doc.Value<RavenJObject>("@metadata"))
												.Select(doc => doc.Value<string>("@id"))
												.ToList();

				Assert.Equal(4,foundDocKeys.Count);
				Assert.Contains("FooBar3", foundDocKeys);
				Assert.Contains("FooBar21", foundDocKeys);
				Assert.Contains("FooBar5", foundDocKeys);
				Assert.Contains("FooBar6", foundDocKeys);
			}
		}

		[Theory]
        [PropertyData("Storages")]
		public void GetDocumentsWithIdStartingWith_Should_Not_Count_Excluded_Docs_WithNonZeroStart(string requestedStorage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Documents.Put("FooBarAA", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarBB", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarCC", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarDD", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarDA", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarEE", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarFF", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarGG", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarHH", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Documents.Put("FooBarKK", null, new RavenJObject(), new RavenJObject(), null);

				int nextPageStart = 0;
                var fetchedDocuments = documentStore.DocumentDatabase.Documents.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "*A", 2, 4, CancellationToken.None, ref nextPageStart);

				var documentsList = fetchedDocuments.ToList();
				var foundDocKeys = documentsList.Select(doc => doc.Value<RavenJObject>("@metadata"))
												.Select(doc => doc.Value<string>("@id"))
												.ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Contains("FooBarDD", foundDocKeys);
				Assert.Contains("FooBarEE", foundDocKeys);
				Assert.Contains("FooBarFF", foundDocKeys);
				Assert.Contains("FooBarGG", foundDocKeys);
			}
		}
	
	}
}

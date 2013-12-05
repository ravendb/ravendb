using System.Linq;

using Raven.Client;
using Raven.Json.Linq;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1379_Client : RavenTest
	{
	    [Theory]
	    [InlineData("voron")]
	    [InlineData("esent")]
	    public void PagingWithoutFilters(string requestedStorage)
	    {
	        using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
	        {
	            documentStore.DocumentDatabase.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
	            documentStore.DocumentDatabase.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

		        var fetchedDocuments = documentStore
					.DatabaseCommands
					.StartsWith("FooBar", string.Empty, 0, 4, exclude: string.Empty)
                    .ToList();

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
                                                   .ToList();

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);
                Assert.Contains("FooBar111", foundDocKeys);
                Assert.Contains("FooBar12", foundDocKeys);

				fetchedDocuments = documentStore
					.DatabaseCommands
					.StartsWith("FooBar", string.Empty, 4, 4, exclude: string.Empty)
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

				fetchedDocuments = documentStore
					.DatabaseCommands
					.StartsWith("FooBar", string.Empty, 8, 4, exclude: string.Empty)
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar8", foundDocKeys);
	        }
	    }

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public void PagingWithoutFiltersWithPagingInformation(string requestedStorage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

				var pagingInformation = new RavenPagingInformation();
				var fetchedDocuments = documentStore
					.DatabaseCommands
					.StartsWith("FooBar", string.Empty, 0, 4, pagingInformation: pagingInformation, exclude: string.Empty)
					.ToList();

				var foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Contains("FooBar1", foundDocKeys);
				Assert.Contains("FooBar11", foundDocKeys);
				Assert.Contains("FooBar111", foundDocKeys);
				Assert.Contains("FooBar12", foundDocKeys);

				fetchedDocuments = documentStore
					.DatabaseCommands
					.StartsWith("FooBar", string.Empty, 4, 4, pagingInformation: pagingInformation, exclude: string.Empty)
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Contains("FooBar21", foundDocKeys);
				Assert.Contains("FooBar3", foundDocKeys);
				Assert.Contains("FooBar5", foundDocKeys);
				Assert.Contains("FooBar6", foundDocKeys);

				fetchedDocuments = documentStore
					.DatabaseCommands
					.StartsWith("FooBar", string.Empty, 8, 4, pagingInformation: pagingInformation, exclude: string.Empty)
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(1, foundDocKeys.Count);
				Assert.Contains("FooBar8", foundDocKeys);
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public void PagingWithoutFiltersAsync(string requestedStorage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

				var fetchedDocuments = documentStore
					.AsyncDatabaseCommands
					.StartsWithAsync("FooBar", string.Empty, 0, 4, exclude: string.Empty)
					.Result
					.ToList();

				var foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Contains("FooBar1", foundDocKeys);
				Assert.Contains("FooBar11", foundDocKeys);
				Assert.Contains("FooBar111", foundDocKeys);
				Assert.Contains("FooBar12", foundDocKeys);

				fetchedDocuments = documentStore
					.AsyncDatabaseCommands
					.StartsWithAsync("FooBar", string.Empty, 4, 4, exclude: string.Empty)
					.Result
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Contains("FooBar21", foundDocKeys);
				Assert.Contains("FooBar3", foundDocKeys);
				Assert.Contains("FooBar5", foundDocKeys);
				Assert.Contains("FooBar6", foundDocKeys);

				fetchedDocuments = documentStore
					.AsyncDatabaseCommands
					.StartsWithAsync("FooBar", string.Empty, 8, 4, exclude: string.Empty)
					.Result
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(1, foundDocKeys.Count);
				Assert.Contains("FooBar8", foundDocKeys);
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public void PagingWithoutFiltersWithPagingInformationAsync(string requestedStorage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

				var pagingInformation = new RavenPagingInformation();
				var fetchedDocuments = documentStore
					.AsyncDatabaseCommands
					.StartsWithAsync("FooBar", string.Empty, 0, 4, pagingInformation: pagingInformation, exclude: string.Empty)
					.Result
					.ToList();

				var foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Contains("FooBar1", foundDocKeys);
				Assert.Contains("FooBar11", foundDocKeys);
				Assert.Contains("FooBar111", foundDocKeys);
				Assert.Contains("FooBar12", foundDocKeys);

				fetchedDocuments = documentStore
					.AsyncDatabaseCommands
					.StartsWithAsync("FooBar", string.Empty, 4, 4, pagingInformation: pagingInformation, exclude: string.Empty)
					.Result
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Contains("FooBar21", foundDocKeys);
				Assert.Contains("FooBar3", foundDocKeys);
				Assert.Contains("FooBar5", foundDocKeys);
				Assert.Contains("FooBar6", foundDocKeys);

				fetchedDocuments = documentStore
					.AsyncDatabaseCommands
					.StartsWithAsync("FooBar", string.Empty, 8, 4, pagingInformation: pagingInformation, exclude: string.Empty)
					.Result
					.ToList();

				foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
												   .ToList();

				Assert.Equal(1, foundDocKeys.Count);
				Assert.Contains("FooBar8", foundDocKeys);
			}
		}

        [Theory]
        [InlineData("voron")]
        [InlineData("esent")]
        public void PagingWithExcludes(string requestedStorage)
        {
            using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DocumentDatabase.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

	            int nextPageStart = 0;
	            var fetchedDocuments = documentStore
                    .DocumentDatabase
					.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 0, 2, ref nextPageStart)
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
					.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 2, 2, ref nextPageStart)
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
					.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 4, 2, ref nextPageStart)
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
        [InlineData("voron")]
        [InlineData("esent")]
        public void PagingWithMatches(string requestedStorage)
        {
            using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DocumentDatabase.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
                documentStore.DocumentDatabase.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

	            int nextPageStart = 0;
	            var fetchedDocuments = documentStore
                    .DocumentDatabase
					.GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 0, 2, ref nextPageStart)
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
					.GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 2, 1, ref nextPageStart)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar111", foundDocKeys);

				nextPageStart = 0;
                fetchedDocuments = documentStore
                    .DocumentDatabase
					.GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 3, 10, ref nextPageStart)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar12", foundDocKeys);
            }
        }

	    [Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public void GetDocumentsWithIdStartingWith_Should_Not_Count_Excluded_Docs(string requestedStorage)
		{
			using(var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

				int nextPageStart = 0;
				var fetchedDocuments = documentStore.DocumentDatabase.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 0, 4, ref nextPageStart);

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
		[InlineData("voron")]
		[InlineData("esent")]
		public void GetDocumentsWithIdStartingWith_Should_Not_Count_Excluded_Docs_WithNonZeroStart(string requestedStorage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Put("FooBarAA", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarBB", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarCC", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarDD", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarDA", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarEE", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarFF", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarGG", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarHH", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarKK", null, new RavenJObject(), new RavenJObject(), null);

				int nextPageStart = 0;
				var fetchedDocuments = documentStore.DocumentDatabase.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "*A", 2, 4, ref nextPageStart);

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

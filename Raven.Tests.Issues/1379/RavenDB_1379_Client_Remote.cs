using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Client;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1379_Client_Remote : RavenTest
	{
	    [Theory]
        [PropertyData("Storages")]
	    public void PagingWithoutFilters(string requestedStorage)
	    {
	        using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
	        {
	            documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

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
        [PropertyData("Storages")]
		public void PagingWithoutFiltersWithPagingInformation(string requestedStorage)
		{
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
			{
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

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
        [PropertyData("Storages")]
		public void PagingWithoutFiltersAsync(string requestedStorage)
		{
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
			{
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

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
        [PropertyData("Storages")]
		public void PagingWithoutFiltersWithPagingInformationAsync(string requestedStorage)
		{
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
			{
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

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
        [PropertyData("Storages")]
        public void PagingWithExcludesWithPagingInformation(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

                var pagingInformation = new RavenPagingInformation();
                var fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StartsWith("FooBar", string.Empty, 0, 2, pagingInformation: pagingInformation, exclude: "1*")
                    .ToList();

                Assert.Equal(0, pagingInformation.Start);
                Assert.Equal(2, pagingInformation.PageSize);
                Assert.Equal(6, pagingInformation.NextPageStart);

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
                                                   .ToList();

                Assert.Equal(2, foundDocKeys.Count);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);

                fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StartsWith("FooBar", string.Empty, 2, 2, pagingInformation: pagingInformation, exclude: "1*")
                    .ToList();

                Assert.Equal(2, pagingInformation.Start);
                Assert.Equal(2, pagingInformation.PageSize);
                Assert.Equal(8, pagingInformation.NextPageStart);

                foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
                                                   .ToList();

                Assert.Equal(2, foundDocKeys.Count);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

                fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StartsWith("FooBar", string.Empty, 4, 2, pagingInformation: pagingInformation, exclude: "1*")
                    .ToList();

                Assert.Equal(4, pagingInformation.Start);
                Assert.Equal(2, pagingInformation.PageSize);
                Assert.Equal(8, pagingInformation.NextPageStart);

                foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
                                                   .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar8", foundDocKeys);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PagingWithMatchesWithPagingInformation(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

                var pagingInformation = new RavenPagingInformation();
                var fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StartsWith("FooBar", "1*", 0, 2, pagingInformation: pagingInformation)
                    .ToList();

                Assert.Equal(0, pagingInformation.Start);
                Assert.Equal(2, pagingInformation.PageSize);
                Assert.Equal(2, pagingInformation.NextPageStart);

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
                                                   .ToList();

                Assert.Equal(2, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);

                fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StartsWith("FooBar", "1*", 2, 1, pagingInformation: pagingInformation)
                    .ToList();

                Assert.Equal(2, pagingInformation.Start);
                Assert.Equal(1, pagingInformation.PageSize);
                Assert.Equal(3, pagingInformation.NextPageStart);

                foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
                                                   .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar111", foundDocKeys);

                fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StartsWith("FooBar", "1*", 3, 10, pagingInformation: pagingInformation)
                    .ToList();

                Assert.Equal(3, pagingInformation.Start);
                Assert.Equal(10, pagingInformation.PageSize);
                Assert.Equal(3, pagingInformation.NextPageStart);

                foundDocKeys = fetchedDocuments.Select(doc => doc.Key)
                                                   .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar12", foundDocKeys);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void StreamingWithoutPagingInformation(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

                var fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StreamDocs(startsWith: "FooBar", matches: string.Empty, start: 0, pageSize: 4, exclude: string.Empty);

                var foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNext())
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);
                Assert.Contains("FooBar111", foundDocKeys);
                Assert.Contains("FooBar12", foundDocKeys);

                fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StreamDocs(startsWith: "FooBar", matches: string.Empty, start: 4, pageSize: 4, exclude: string.Empty);

                foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNext())
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

                fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StreamDocs(startsWith: "FooBar", matches: string.Empty, start: 8, pageSize: 4, exclude: string.Empty);

                foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNext())
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar8", foundDocKeys);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void StreamingWithPagingInformation(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

                var pagingInformation = new RavenPagingInformation();
                var fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StreamDocs(startsWith: "FooBar", matches: string.Empty, start: 0, pageSize: 4, exclude: string.Empty, pagingInformation: pagingInformation);

                var foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNext())
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);
                Assert.Contains("FooBar111", foundDocKeys);
                Assert.Contains("FooBar12", foundDocKeys);

                fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StreamDocs(startsWith: "FooBar", matches: string.Empty, start: 4, pageSize: 4, exclude: string.Empty, pagingInformation: pagingInformation);

                foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNext())
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

                fetchedDocuments = documentStore
                    .DatabaseCommands
                    .StreamDocs(startsWith: "FooBar", matches: string.Empty, start: 8, pageSize: 4, exclude: string.Empty, pagingInformation: pagingInformation);

                foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNext())
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar8", foundDocKeys);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void StreamingWithoutPagingInformationAsync(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

                var fetchedDocuments = documentStore
                    .AsyncDatabaseCommands
                    .StreamDocsAsync(startsWith: "FooBar", matches: string.Empty, start: 0, pageSize: 4, exclude: string.Empty).Result;

                var foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNextAsync().Result)
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);
                Assert.Contains("FooBar111", foundDocKeys);
                Assert.Contains("FooBar12", foundDocKeys);

                fetchedDocuments = documentStore
                    .AsyncDatabaseCommands
                    .StreamDocsAsync(startsWith: "FooBar", matches: string.Empty, start: 4, pageSize: 4, exclude: string.Empty).Result;

                foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNextAsync().Result)
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

                fetchedDocuments = documentStore
                    .AsyncDatabaseCommands
                    .StreamDocsAsync(startsWith: "FooBar", matches: string.Empty, start: 8, pageSize: 4, exclude: string.Empty).Result;

                foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNextAsync().Result)
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar8", foundDocKeys);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void StreamingWithPagingInformationAsync(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                documentStore.DatabaseCommands.Put("FooBar1", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo2", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar3", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar11", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar12", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar21", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar5", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo7", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar111", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("BarFoo6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar6", null, new RavenJObject(), new RavenJObject());
                documentStore.DatabaseCommands.Put("FooBar8", null, new RavenJObject(), new RavenJObject());

                var pagingInformation = new RavenPagingInformation();
                var fetchedDocuments = documentStore
                    .AsyncDatabaseCommands
                    .StreamDocsAsync(startsWith: "FooBar", matches: string.Empty, start: 0, pageSize: 4, exclude: string.Empty, pagingInformation: pagingInformation).Result;

                var foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNextAsync().Result)
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);
                Assert.Contains("FooBar111", foundDocKeys);
                Assert.Contains("FooBar12", foundDocKeys);

                fetchedDocuments = documentStore
                    .AsyncDatabaseCommands
                    .StreamDocsAsync(startsWith: "FooBar", matches: string.Empty, start: 4, pageSize: 4, exclude: string.Empty, pagingInformation: pagingInformation).Result;

                foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNextAsync().Result)
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

                fetchedDocuments = documentStore
                    .AsyncDatabaseCommands
                    .StreamDocsAsync(startsWith: "FooBar", matches: string.Empty, start: 8, pageSize: 4, exclude: string.Empty, pagingInformation: pagingInformation).Result;

                foundDocKeys = new List<string>();

                while (fetchedDocuments.MoveNextAsync().Result)
                    foundDocKeys.Add(fetchedDocuments.Current["@metadata"].Value<string>("@id"));

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar8", foundDocKeys);
            }
        }
	}
}

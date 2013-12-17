// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1379.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
    using System.Linq;

    using Raven.Json.Linq;

    using Xunit;
    using Xunit.Extensions;

    public class RavenDB_1379 : RavenTest
    {
        [Theory]
        [InlineData("esent")]
        public void ShouldNotReturnExludeBasedResults(string requestedStorage)
        {
            var ids = new[]
                {
                    "users/aaa/1",
                    "users/aaa/revisions/1",
                    "users/bbb/1",
                    "users/bbb/revisions/1",
                    "users/bbb/revisions/2",
                    "users/ccc/1"
                };

            using (var store = NewDocumentStore(requestedStorage: requestedStorage))
            {
                using (var session = store.OpenSession())
                {
                    foreach (var id in ids)
                    {
                        session.Store(new { id }, id);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var results = session.Advanced.LoadStartingWithAsync<dynamic>(
                        keyPrefix: "users/",
                        start: 0,
                        pageSize: 3,
                        exclude: "*/revisions/*");

                    Assert.Equal(3, results.Result.Count());

                    var resultIds = results.Result
                        .Select(x => (string)x.id)
                        .ToList();

                    Assert.Equal(true, resultIds.Contains("users/aaa/1"));
                    Assert.Equal(true, resultIds.Contains("users/bbb/1"));
                    Assert.Equal(true, resultIds.Contains("users/ccc/1"));
                }
            }
        }

        [Theory]
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
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, 0, 4)
                    .ToList();

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                                   .Select(doc => doc.Value<string>("@id"))
                                                   .ToList();

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);
                Assert.Contains("FooBar111", foundDocKeys);
                Assert.Contains("FooBar12", foundDocKeys);

                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, 4, 4)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, string.Empty, 8, 4)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar8", foundDocKeys);
            }
        }

        [Theory]
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

                var fetchedDocuments = documentStore
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 0, 2)
                    .ToList();

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                                   .Select(doc => doc.Value<string>("@id"))
                                                   .ToList();

                Assert.Equal(2, foundDocKeys.Count);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar3", foundDocKeys);

                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 2, 2)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(2, foundDocKeys.Count);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);

                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 4, 2)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar8", foundDocKeys);
            }
        }

        [Theory]
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

                var fetchedDocuments = documentStore
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 0, 2)
                    .ToList();

                var foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                                   .Select(doc => doc.Value<string>("@id"))
                                                   .ToList();

                Assert.Equal(2, foundDocKeys.Count);
                Assert.Contains("FooBar1", foundDocKeys);
                Assert.Contains("FooBar11", foundDocKeys);

                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 2, 1)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar111", foundDocKeys);

                fetchedDocuments = documentStore
                    .DocumentDatabase
                    .GetDocumentsWithIdStartingWith("FooBar", "1*", string.Empty, 3, 10)
                    .ToList();

                foundDocKeys = fetchedDocuments.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                               .Select(doc => doc.Value<string>("@id"))
                                               .ToList();

                Assert.Equal(1, foundDocKeys.Count);
                Assert.Contains("FooBar12", foundDocKeys);
            }
        }

        [Theory]
        [InlineData("esent")]
        public void GetDocumentsWithIdStartingWith_Should_Not_Count_Excluded_Docs(string requestedStorage)
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

                var fetchedDocuments = documentStore.DocumentDatabase.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 0, 4);

                var documentsList = fetchedDocuments.ToList();
                var foundDocKeys = documentsList.Select(doc => doc.Value<RavenJObject>("@metadata"))
                                                .Select(doc => doc.Value<string>("@id"))
                                                .ToList();

                Assert.Equal(4, foundDocKeys.Count);
                Assert.Contains("FooBar3", foundDocKeys);
                Assert.Contains("FooBar21", foundDocKeys);
                Assert.Contains("FooBar5", foundDocKeys);
                Assert.Contains("FooBar6", foundDocKeys);
            }
        }

        [Theory]
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

                var fetchedDocuments = documentStore.DocumentDatabase.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "*A", 2, 4);

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
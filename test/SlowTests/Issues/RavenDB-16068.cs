using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16068 : RavenTestBase
    {
        public RavenDB_16068(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task PatchByIdQuery(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                var idsList = new List<string>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < 1200; i++)
                    {
                        var id = (i + 1).ToString();
                        idsList.Add(id);
                        await bulkInsert.StoreAsync(new User(), id);
                    }
                }

                var query = $"{baseQuery} where id() in (";
                var first = true;
                foreach (var id in idsList)
                {
                    if (first == false)
                        query += ",";

                    first = false;
                    query += $"'{id}'";
                }
                query += ")";

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = query }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30000));

                using (var session = store.OpenAsyncSession())
                {
                    var docsCount = await session.Query<User>().CountAsync();
                    Assert.Equal(0, docsCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task PatchByIdQueryWithUpdatedDocument(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                var idsList = new List<string>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < 1200; i++)
                    {
                        var id = (i + 1).ToString();
                        idsList.Add(id);
                        await bulkInsert.StoreAsync(new User(), id);
                    }
                }

                await SetAction(store, "840");

                var query = $"{baseQuery} where id() in (";
                var first = true;
                foreach (var id in idsList)
                {
                    if (first == false)
                        query += ",";

                    first = false;
                    query += $"'{id}'";
                }
                query += ")";

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = query }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30000));

                using (var session = store.OpenAsyncSession())
                {
                    var docsCount = await session.Query<User>().CountAsync();
                    Assert.Equal(0, docsCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task DeleteByIdQuery(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                var idsList = new List<string>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < 1200; i++)
                    {
                        var id = i.ToString();
                        idsList.Add(id);
                        await bulkInsert.StoreAsync(new User(), id);
                    }
                }

                var query = $"{baseQuery} where id() in (";
                var first = true;
                foreach (var id in idsList)
                {
                    if (first == false)
                        query += ",";

                    first = false;
                    query += $"'{id}'";
                }
                query += ")";

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = query }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30000));

                using (var session = store.OpenAsyncSession())
                {
                    var docsCount = await session.Query<User>().CountAsync();
                    Assert.Equal(0, docsCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task DeleteByIdQueryWithUpdatedDocument(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                var idsList = new List<string>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < 1200; i++)
                    {
                        var id = i.ToString();
                        idsList.Add(id);
                        await bulkInsert.StoreAsync(new User(), id);
                    }
                }

                await SetAction(store, "840");

                var query = $"{baseQuery} where id() in (";
                var first = true;
                foreach (var id in idsList)
                {
                    if (first == false)
                        query += ",";

                    first = false;
                    query += $"'{id}'";
                }
                query += ")";

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = query }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30000));

                using (var session = store.OpenAsyncSession())
                {
                    var docsCount = await session.Query<User>().CountAsync();
                    Assert.Equal(0, docsCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task DeleteByIdQueryWithDeletedDocument(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                var idsList = new List<string>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < 1200; i++)
                    {
                        var id = i.ToString();
                        idsList.Add(id);
                        await bulkInsert.StoreAsync(new User(), id);
                    }
                }

                await SetAction(store, "840", delete: true);

                var query = $"{baseQuery} where id() in (";
                var first = true;
                foreach (var id in idsList)
                {
                    if (first == false)
                        query += ",";

                    first = false;
                    query += $"'{id}'";
                }
                query += ")";

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = query }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30000));

                using (var session = store.OpenAsyncSession())
                {
                    var docsCount = await session.Query<User>().CountAsync();
                    Assert.Equal(0, docsCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task PatchByStartsWithQuery(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                const int totalDocs = 1024 * 2;
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < totalDocs; i++)
                    {
                        await bulkInsert.StoreAsync(new User());
                    }
                }

                var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery { Query = $"{baseQuery} where StartsWith(id(),'users/') UPDATE {{this.Name = 'Grisha'}}" }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30));

                using (var session = store.OpenAsyncSession())
                {
                    var docs = await session.Query<User>().ToListAsync();
                    Assert.Equal(totalDocs, docs.Count);

                    Assert.All(docs.Select(x => x.Name), x => x.Equals("Grisha"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task PatchByStartsWithQueryWithNewDocument(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                const int totalDocs = 1024 * 2;
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < totalDocs; i++)
                    {
                        await bulkInsert.StoreAsync(new User(), $"users/{i + 1}");
                    }
                }

                await SetAction(store, "users/1921");

                var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery { Query = $"{baseQuery} where StartsWith(id(),'users/') UPDATE {{this.Name = 'Grisha'}}" }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30000));

                using (var session = store.OpenAsyncSession())
                {
                    var docs = await session.Query<User>().ToListAsync();
                    Assert.Equal(totalDocs, docs.Count);

                    Assert.All(docs.Select(x => x.Name), x => x.Equals("Grisha"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task DeleteByStartsWithQuery(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < 1024 * 2; i++)
                    {
                        await bulkInsert.StoreAsync(new User());
                    }
                }

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery {Query = $"{baseQuery} where StartsWith(id(),'users/')" }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30));

                using (var session = store.OpenAsyncSession())
                {
                    var docsCount = await session.Query<User>().CountAsync();
                    Assert.Equal(0, docsCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task DeleteByStartsWithQueryWithNewDocument(Options options, string baseQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < 1024 * 2; i++)
                    {
                        await bulkInsert.StoreAsync(new User(), $"users/{i + 1}");
                    }
                }

                await SetAction(store, "users/1921");

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = $"{baseQuery} where StartsWith(id(),'users/')" }));
                operation.WaitForCompletion<BulkOperationResult>(TimeSpan.FromSeconds(30));

                using (var session = store.OpenAsyncSession())
                {
                    var docsCount = await session.Query<User>().CountAsync();
                    Assert.Equal(0, docsCount);
                }
            }
        }

        private async Task SetAction(DocumentStore store, string documentId, bool delete = false)
        {
            var count = 0;
            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

            database.ForTestingPurposesOnly().CollectionRunnerBeforeOpenReadTransaction = () =>
            {
                if (++count != 2)
                    return;

                using (var session = store.OpenSession())
                {
                    if (delete)
                    {
                        session.Delete(documentId);
                    }
                    else
                    {
                        session.Store(new User(), documentId);
                    }

                    session.SaveChanges();
                }
            };
        }
    }
}

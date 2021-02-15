using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16068 : RavenTestBase
    {
        public RavenDB_16068(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task PatchByIdQuery(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task PatchByIdQueryWithUpdatedDocument(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task DeleteByIdQuery(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task DeleteByIdQueryWithUpdatedDocument(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task DeleteByIdQueryWithDeletedDocument(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task PatchByStartsWithQuery(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task PatchByStartsWithQueryWithNewDocument(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task DeleteByStartsWithQuery(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

        [Theory]
        [InlineData("from Users")]
        [InlineData("from @all_docs")]
        public async Task DeleteByStartsWithQueryWithNewDocument(string baseQuery)
        {
            using (var store = GetDocumentStore())
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

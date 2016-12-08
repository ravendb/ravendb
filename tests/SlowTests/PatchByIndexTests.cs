using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Client.Commands;
using Raven.Server.Config;
using Sparrow.Json;
using Xunit;

namespace NewClientTests.NewClient.FastTests.Patching
{
    public class PatchByIndexTests : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string LastName { get; set; }

            public string FullName { get; set; }
        }

        private class CustomType
        {
            public string Id { get; set; }
            public string Owner { get; set; }
            public int Value { get; set; }
            public List<string> Comments { get; set; }
        }

        private readonly CustomType _test = new CustomType
        {
            Id = "someId",
            Owner = "bob",
            Value = 12143,
            Comments = new List<string>(new[] { "one", "two", "seven" })
        };

        //splice(2, 1) will remove 1 elements from position 2 onwards (zero-based)
        private string sampleScript = @"
    this.Comments.splice(2, 1);
    this.Id = 'Something new'; 
    this.Value++; 
    this.newValue = ""err!!"";
    this.Comments = this.Comments.Map(function(comment) {   
        return (comment == ""one"") ? comment + "" test"" : comment;
    });";


        [Fact]
        public async Task CanPerformAdvancedWithSetBasedUpdates()
        {
            using (var store = GetDocumentStore())
            {
                var item1 = new CustomType
                {
                    Id = "someId/1",
                    Owner = "bob",
                    Value = 12143,
                    Comments = new List<string>(new[] { "one", "two", "seven" })
                };
                var item2 = new CustomType
                {
                    Id = "someId/2",
                    Owner = "NOT bob",
                    Value = 9999,
                    Comments = new List<string>(new[] { "one", "two", "seven" })
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(item1);
                    await session.StoreAsync(item2);
                    await session.SaveChangesAsync();

                }

                JsonOperationContext context;
                store.GetRequestExecuter(store.DefaultDatabase).ContextPool.AllocateOperationContext(out context);

                var putIndexOperation = new PutIndexOperation(context);
                var indexCommand = putIndexOperation.CreateRequest(store.Conventions, "TestIndex", new IndexDefinition
                { Maps = { @"from doc in docs.CustomTypes 
                                     select new { doc.Owner }" } });
                if (indexCommand != null)
                    store.GetRequestExecuter(store.DefaultDatabase).Execute(indexCommand, context);

                WaitForIndexing(store);
                store.OpenSession().Advanced.DocumentQuery<CustomType>("TestIndex")
                    .WaitForNonStaleResults().ToList();

                var patchByIndexOperation = new PatchByIndexOperation(context);
                var patchCommand = patchByIndexOperation.CreateRequest("TestIndex",
                    new IndexQuery { Query = "Owner:bob" },
                    null, new PatchRequest { Script = sampleScript }, store);
                if (patchCommand != null)
                    store.GetRequestExecuter(store.DefaultDatabase).Execute(patchCommand, context);

                var getDocumentCommand = new GetDocumentCommand
                {
                    Ids = new[] { item1.Id },
                    Context = context
                };
                store.GetRequestExecuter(store.DefaultDatabase).Execute(getDocumentCommand, context);

                var results = getDocumentCommand.Result.Results;
                Assert.Equal(1, results.Length);
                var res = (BlittableJsonReaderObject)results[0];
                object obj;
                res.TryGetMember("Comments", out obj);
                Assert.Equal(2, ((BlittableJsonReaderArray)obj).Length);
                Assert.Equal("one test", ((BlittableJsonReaderArray)obj).GetStringByIndex(0));
                Assert.Equal("two", ((BlittableJsonReaderArray)obj).GetStringByIndex(1));
                res.TryGetMember("Value", out obj);
                Assert.Equal(12144, ((Int64)obj));
                res.TryGetMember("newValue", out obj);
                Assert.Equal("err!!", obj.ToString());


                getDocumentCommand = new GetDocumentCommand
                {
                    Ids = new[] { item2.Id },
                    Context = context
                };
                store.GetRequestExecuter(store.DefaultDatabase).Execute(getDocumentCommand, context);

                results = getDocumentCommand.Result.Results;
                Assert.Equal(1, results.Length);
                res = (BlittableJsonReaderObject)results[0];
                res.TryGetMember("Comments", out obj);
                Assert.Equal(3, ((BlittableJsonReaderArray)obj).Length);
                Assert.Equal("one", ((BlittableJsonReaderArray)obj).GetStringByIndex(0));
                Assert.Equal("two", ((BlittableJsonReaderArray)obj).GetStringByIndex(1));
                Assert.Equal("seven", ((BlittableJsonReaderArray)obj).GetStringByIndex(2));
                res.TryGetMember("Value", out obj);
                Assert.Equal(9999, ((Int64)obj));

            }
        }

        [Fact]
        public async Task CanCreateDocumentsIfPatchingAppliedByIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType
                    {
                        Id = "Item/1",
                        Value = 1
                    });
                    await session.StoreAsync(new CustomType
                    {
                        Id = "Item/2",
                        Value = 2
                    });
                    await session.SaveChangesAsync();
                }

                JsonOperationContext context;
                store.GetRequestExecuter(store.DefaultDatabase).ContextPool.AllocateOperationContext(out context);

                var putIndexOperation = new PutIndexOperation(context);
                var indexCommand = putIndexOperation.CreateRequest(store.Conventions, "TestIndex", new IndexDefinition
                { Maps = { @"from doc in docs.CustomTypes 
                                     select new { doc.Value }" } });
                if (indexCommand != null)
                    store.GetRequestExecuter(store.DefaultDatabase).Execute(indexCommand, context);



                using (var session = store.OpenAsyncSession())
                {
                    await session.Advanced.AsyncDocumentQuery<CustomType>("TestIndex")
                        .WaitForNonStaleResults()
                        .ToListAsync();
                }

                var patchByIndexOperation = new PatchByIndexOperation(context);
                var patchCommand = patchByIndexOperation.CreateRequest("TestIndex",
                    new IndexQuery { Query = "Value:1" },
                    null, new PatchRequest { Script = @"PutDocument('NewItem/3', {'CopiedValue': this.Value });" }, store);
                if (patchCommand != null)
                    store.GetRequestExecuter(store.DefaultDatabase).Execute(patchCommand, context);

                var getDocumentCommand = new GetDocumentCommand
                {
                    Ids = new[] { "Item/1", "Item/2" },
                    Context = context
                };
                store.GetRequestExecuter(store.DefaultDatabase).Execute(getDocumentCommand, context);

                var results = getDocumentCommand.Result.Results;
                Assert.Equal(2, results.Length);

                getDocumentCommand = new GetDocumentCommand
                {
                    Ids = new[] { "NewItem/3" },
                    Context = context
                };
                store.GetRequestExecuter(store.DefaultDatabase).Execute(getDocumentCommand, context);

                results = getDocumentCommand.Result.Results;
                Assert.Equal(1, results.Length);
                var res = (BlittableJsonReaderObject)results[0];
                object obj;
                res.TryGetMember("CopiedValue", out obj);
                Assert.Equal(1, ((Int64)obj));
            }
        }

        [Fact]
        public async Task AwaitAsyncPatchByIndexShouldWork()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document => document.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"))
            {


                RavenQueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Name == "John")
                        .ToList();
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "First #" + i,
                            LastName = "Last #" + i
                        }, "users/" + i);
                        await session.SaveChangesAsync();
                    }
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));

                JsonOperationContext context;
                store.GetRequestExecuter(store.DefaultDatabase).ContextPool.AllocateOperationContext(out context);

                var patchByIndexOperation = new PatchByIndexOperation(context);
                var patchCommand = patchByIndexOperation.CreateRequest(stats.IndexName,
                    new IndexQuery { Query = string.Empty },
                    null, new PatchRequest { Script = "this.FullName = this.FirstName + ' ' + this.LastName;" }, store);
                if (patchCommand != null)
                    await store.GetRequestExecuter(store.DefaultDatabase).ExecuteAsync(patchCommand, context);

                using (var db = store.OpenAsyncSession())
                {
                    var lastUser = await db.LoadAsync<User>("users/29");
                    Assert.NotNull(lastUser.FullName);
                }
            }
        }
    }
}
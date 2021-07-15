using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class ShardedHiloTests : ShardedTestBase
    {
        public ShardedHiloTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStoreWithoutId()
        {
            using (var store = GetShardedDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Aviv" };
                    session.Store(user);

                    id = user.Id;
                    Assert.NotNull(id);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(id);
                    Assert.Equal("Aviv", loaded.Name);
                }
            }
        }

        [Fact]
        public async Task Hilo_Cannot_Go_Down()
        {
            const string hiloId = "Raven/Hilo/users";

            using (var store = GetShardedDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                var numOfShards = record.Shards?.Length ?? 0;
                Assert.Equal(3, numOfShards);

                var djv = new DynamicJsonValue
                {
                    ["Max"] = 32
                };

                var conventions = new DocumentConventions();

                // save hilo doc on each shard, with Max = 32
                for (int i = 0; i < numOfShards; i++)
                {
                    using var re = RequestExecutor.Create(
                        store.Urls,
                        store.Database + "$" + i,
                        store.Certificate,
                        conventions);

                    PutDocumentCommand cmd;
                    using (re.ContextPool.AllocateOperationContext(out var context))
                    {
                        var doc = context.ReadObject(djv, "hilo");
                        cmd = new PutDocumentCommand(hiloId, changeVector: null, doc);
                        await re.ExecuteAsync(cmd, context);
                    }

                    Assert.Equal(hiloId, cmd.Result.Id);
                }

                var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", (DocumentStore)store, store.Database,
                    store.Conventions.IdentityPartsSeparator);

                var docId = await hiLoKeyGenerator.GenerateDocumentIdAsync(null);
                Assert.True(docId.StartsWith("users/33"));
                var ids = new HashSet<string> { docId };

                for (int i = 0; i < 128; i++)
                {
                    var nextId = await hiLoKeyGenerator.GenerateDocumentIdAsync(null);
                    Assert.True(ids.Add(nextId), "Failed at " + i);
                }

                var collection = ids
                    .GroupBy(x => x)
                    .Select(g => new
                    {
                        g.Key,
                        Count = g.Count()
                    })
                    .Where(x => x.Count > 1);

                Assert.Empty(collection);
            }
        }

        [Fact]
        public async Task HiLo_Async_MultiDb()
        {
            using (var store = GetShardedDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                var numOfShards = record.Shards?.Length ?? 0;
                Assert.Equal(3, numOfShards);

                var usersHiloDjv = new DynamicJsonValue
                {
                    ["Max"] = 64
                };

                var productsHiloDjv = new DynamicJsonValue
                {
                    ["Max"] = 128
                };

                var conventions = new DocumentConventions();

                // save hilo doc on each shard, with Max = 32
                for (int i = 0; i < numOfShards; i++)
                {
                    using var re = RequestExecutor.Create(
                        store.Urls,
                        store.Database + "$" + i,
                        store.Certificate,
                        conventions);

                    PutDocumentCommand cmd;
                    using (re.ContextPool.AllocateOperationContext(out var context))
                    {
                        var doc = context.ReadObject(usersHiloDjv, "hilo");
                        cmd = new PutDocumentCommand("Raven/Hilo/users", changeVector: null, doc);
                        await re.ExecuteAsync(cmd, context);
                    }

                    Assert.Equal("Raven/Hilo/users", cmd.Result.Id);

                    using (re.ContextPool.AllocateOperationContext(out var context))
                    {
                        var doc = context.ReadObject(productsHiloDjv, "hilo");
                        cmd = new PutDocumentCommand("Raven/Hilo/products", changeVector: null, doc);
                        await re.ExecuteAsync(cmd, context);
                    }

                    Assert.Equal("Raven/Hilo/products", cmd.Result.Id);
                }

                var multiDbHiLo = new AsyncMultiDatabaseHiLoIdGenerator((DocumentStore)store);
                var generateDocumentKey = await multiDbHiLo.GenerateDocumentIdAsync(null, new User());
                Assert.True(generateDocumentKey.StartsWith("users/65"));

                generateDocumentKey = await multiDbHiLo.GenerateDocumentIdAsync(null, new Product());
                Assert.True(generateDocumentKey.StartsWith("products/129"));
            }
        }

        [Fact]
        public async Task Capacity_Should_Double()
        {
            const string hiloId = "Raven/Hilo/users";
            const int initialMax = 64;

            using (var store = GetShardedDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                var numOfShards = record.Shards?.Length ?? 0;
                Assert.Equal(3, numOfShards);

                var djv = new DynamicJsonValue
                {
                    ["Max"] = initialMax
                };

                var conventions = new DocumentConventions();

                // save hilo doc on each shard, with Max = 64
                for (int i = 0; i < numOfShards; i++)
                {
                    using var re = RequestExecutor.Create(
                        store.Urls,
                        store.Database + "$" + i,
                        store.Certificate,
                        conventions);

                    PutDocumentCommand cmd;
                    using (re.ContextPool.AllocateOperationContext(out var context))
                    {
                        var doc = context.ReadObject(djv, "hilo");
                        cmd = new PutDocumentCommand(hiloId, changeVector: null, doc);
                        await re.ExecuteAsync(cmd, context);
                    }

                    Assert.Equal(hiloId, cmd.Result.Id);
                }

                var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", (DocumentStore)store, store.Database,
                    store.Conventions.IdentityPartsSeparator);

                for (var i = 0; i < 32; i++)
                    await hiLoKeyGenerator.GenerateDocumentIdAsync(null);

                int? hiloShardIndex = null;
                var maxValues = new long[numOfShards];
                for (int i = 0; i < numOfShards; i++)
                {
                    long max;
                    using (var re = RequestExecutor.Create(
                        store.Urls,
                        record.DatabaseName + "$" + i,
                        store.Certificate,
                        conventions))
                    {
                        var cmd = new GetDocumentsCommand(hiloId, includes: null, metadataOnly: false);
                        using (re.ContextPool.AllocateOperationContext(out var context))
                        {
                            await re.ExecuteAsync(cmd, context);
                        }

                        Assert.Equal(1, cmd.Result.Results.Length);
                        var hiloDoc = cmd.Result.Results[0] as BlittableJsonReaderObject;
                        Assert.NotNull(hiloDoc);
                        Assert.True(hiloDoc.TryGet("Max", out max));
                        maxValues[i] = max;
                    }

                    Assert.True(max == initialMax || max == initialMax + 32);

                    if (max == initialMax)
                    {
                        // this hilo doc was not modified => hilo range was received from a different shard 
                        continue;
                    }

                    // found the shard from which hilo range was received
                    Assert.Null(hiloShardIndex); // ensure that 'Max' was changed on a single shard 
                    hiloShardIndex = i;
                }

                Assert.True(hiloShardIndex.HasValue);

                //we should be receiving a range of 64 now
                await hiLoKeyGenerator.GenerateDocumentIdAsync(null);

                hiloShardIndex = null;
                for (int i = 0; i < numOfShards; i++)
                {
                    long max;
                    using (var re = RequestExecutor.Create(
                        store.Urls,
                        record.DatabaseName + "$" + i,
                        store.Certificate,
                        conventions))
                    {
                        var cmd = new GetDocumentsCommand(hiloId, includes: null, metadataOnly: false);
                        using (re.ContextPool.AllocateOperationContext(out var context))
                        {
                            await re.ExecuteAsync(cmd, context);
                        }

                        Assert.Equal(1, cmd.Result.Results.Length);
                        var hiloDoc = cmd.Result.Results[0] as BlittableJsonReaderObject;
                        Assert.NotNull(hiloDoc);
                        Assert.True(hiloDoc.TryGet("Max", out max));
                    }

                    Assert.True(max == maxValues[i] || max == maxValues[i] + 64);

                    if (max == maxValues[i])
                    {
                        // hilo range was received from a different shard
                        continue;
                    }

                    Assert.Null(hiloShardIndex); // ensure that 'Max' was changed on a single shard 

                    // found the shard from which hilo range was received, and capacity was doubled (max == maxValues[i] + 64)
                    hiloShardIndex = i;
                }

                Assert.True(hiloShardIndex.HasValue);
            }
        }

        [Fact]
        public async Task Return_Unused_Range_On_Dispose()
        {
            const string hiloId = "Raven/Hilo/users";

            using (var store = GetShardedDocumentStore())
            {
                var newStore = new DocumentStore()
                {
                    Urls = store.Urls,
                    Database = store.Database
                };
                newStore.Initialize();

                var record = newStore.Maintenance.Server.Send(new GetDatabaseRecordOperation(newStore.Database));
                var numOfShards = record.Shards?.Length ?? 0;
                Assert.Equal(3, numOfShards);

                var djv = new DynamicJsonValue
                {
                    ["Max"] = 32
                };

                var conventions = new DocumentConventions();

                // save hilo doc on each shard, with Max = 32
                for (int i = 0; i < numOfShards; i++)
                {
                    using var re = RequestExecutor.Create(
                        newStore.Urls,
                        record.DatabaseName + "$" + i,
                        newStore.Certificate,
                        conventions);

                    PutDocumentCommand cmd;
                    using (re.ContextPool.AllocateOperationContext(out var context))
                    {
                        var doc = context.ReadObject(djv, "hilo");
                        cmd = new PutDocumentCommand(hiloId, changeVector: null, doc);
                        await re.ExecuteAsync(cmd, context);
                    }

                    Assert.Equal(hiloId, cmd.Result.Id);
                }

                using (var session = newStore.OpenSession())
                {
                    // we don't know what shard the hilo range is going to come from

                    session.Store(new User());
                    session.Store(new User());

                    session.SaveChanges();
                }

                int? hiloShardIndex = null;
                for (int i = 0; i < 3; i++)
                {
                    long max;
                    using (var re = RequestExecutor.Create(
                        newStore.Urls,
                        record.DatabaseName + "$" + i,
                        newStore.Certificate,
                        conventions))
                    {
                        var cmd = new GetDocumentsCommand(hiloId, includes: null, metadataOnly: false);
                        using (re.ContextPool.AllocateOperationContext(out var context))
                        {
                            await re.ExecuteAsync(cmd, context);
                        }

                        Assert.Equal(1, cmd.Result.Results.Length);
                        var hiloDoc = cmd.Result.Results[0] as BlittableJsonReaderObject;
                        Assert.NotNull(hiloDoc);
                        Assert.True(hiloDoc.TryGet("Max", out max));
                    }

                    Assert.True(max == 32 || max == 64);

                    if (max == 32)
                    {
                        // this hilo doc was not modified => session.Store got it's hilo range from a different shard 
                        continue;
                    }

                    // found the shard from which hilo range was received
                    hiloShardIndex = i;
                    break;
                }

                Assert.True(hiloShardIndex.HasValue);

                newStore.Dispose(); //on document store dispose, hilo-return should be called 

                newStore = new DocumentStore()
                {
                    Urls = store.Urls,
                    Database = store.Database
                };
                newStore.Initialize();

                using (var re = RequestExecutor.Create(
                    newStore.Urls,
                    record.DatabaseName + "$" + hiloShardIndex,
                    newStore.Certificate,
                    conventions))
                {
                    var cmd = new GetDocumentsCommand(hiloId, includes: null, metadataOnly: false);
                    using (re.ContextPool.AllocateOperationContext(out var context))
                    {
                        await re.ExecuteAsync(cmd, context);
                    }

                    Assert.Equal(1, cmd.Result.Results.Length);
                    
                    var hiloDoc = cmd.Result.Results[0] as BlittableJsonReaderObject;
                    Assert.NotNull(hiloDoc);
                    
                    Assert.True(hiloDoc.TryGet("Max", out long max));
                    Assert.True(max == 34); // value of Max is 34 => unused range was returned
                }

                newStore.Dispose();
            }
        }
    }
}

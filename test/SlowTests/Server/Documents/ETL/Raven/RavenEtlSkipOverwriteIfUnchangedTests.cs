using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenEtlSkipOverwriteIfUnchangedTests : RavenTestBase
    {
        public RavenEtlSkipOverwriteIfUnchangedTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ShouldNotRewriteDestinationDocumentWhenTransformedResultIsUnchanged(bool skipOverwriteIfUnchanged)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, skipOverwriteIfUnchanged, "loadToUsers({ Name: this.Name });");

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe", Age = 30 }, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
                var initial = await GetDestinationStateAsync(dest);

                // Age isn't loaded by the script, so the transformed result is identical
                etlDone.Reset();
                await UpdateUserAsync(src, user => user.Age = 31);
                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
                var afterUnloadedChange = await GetDestinationStateAsync(dest);

                if (skipOverwriteIfUnchanged)
                {
                    Assert.Equal(initial.ChangeVector, afterUnloadedChange.ChangeVector);
                    Assert.Equal(initial.LastDocEtag, afterUnloadedChange.LastDocEtag);
                }
                else
                {
                    Assert.NotEqual(initial.ChangeVector, afterUnloadedChange.ChangeVector);
                    Assert.True(afterUnloadedChange.LastDocEtag > initial.LastDocEtag);
                }

                // Name is loaded by the script, so the destination must be updated in both modes
                etlDone.Reset();
                await UpdateUserAsync(src, user => user.Name = "Doe");
                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
                var afterLoadedChange = await GetDestinationStateAsync(dest);

                Assert.NotEqual(afterUnloadedChange.ChangeVector, afterLoadedChange.ChangeVector);
                Assert.True(afterLoadedChange.LastDocEtag > afterUnloadedChange.LastDocEtag);

                using (var session = dest.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("Doe", user.Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task BatchPutShouldKeepIdenticalDocumentWhenSkipOverwriteIfUnchangedIsSet(bool skipOverwriteIfUnchanged)
        {
            using (var store = GetDocumentStore())
            {
                await PutUserAsync(store, "Joe", skipOverwriteIfUnchanged);
                var initial = await GetDestinationStateAsync(store);

                var replyChangeVector = await PutUserAsync(store, "Joe", skipOverwriteIfUnchanged);
                var afterIdenticalPut = await GetDestinationStateAsync(store);

                if (skipOverwriteIfUnchanged)
                {
                    Assert.Equal(initial.ChangeVector, afterIdenticalPut.ChangeVector);
                    Assert.Equal(initial.LastDocEtag, afterIdenticalPut.LastDocEtag);
                    Assert.Equal(initial.ChangeVector, replyChangeVector);
                }
                else
                {
                    Assert.NotEqual(initial.ChangeVector, afterIdenticalPut.ChangeVector);
                    Assert.True(afterIdenticalPut.LastDocEtag > initial.LastDocEtag);
                }

                await PutUserAsync(store, "Doe", skipOverwriteIfUnchanged);
                var afterChangedPut = await GetDestinationStateAsync(store);

                Assert.NotEqual(afterIdenticalPut.ChangeVector, afterChangedPut.ChangeVector);
                Assert.True(afterChangedPut.LastDocEtag > afterIdenticalPut.LastDocEtag);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("Doe", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task ShouldSkipOnShardedDestination()
        {
            using (var src = GetDocumentStore())
            using (var dest = Sharding.GetDocumentStore())
            {
                AddEtl(src, dest, skipOverwriteIfUnchanged: true, "loadToUsers({ Name: this.Name });");

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe", Age = 30 }, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
                var initial = await GetChangeVectorAsync(dest);

                etlDone.Reset();
                await UpdateUserAsync(src, user => user.Age = 31);
                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                Assert.Equal(initial, await GetChangeVectorAsync(dest));

                etlDone.Reset();
                await UpdateUserAsync(src, user => user.Name = "Doe");
                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                Assert.NotEqual(initial, await GetChangeVectorAsync(dest));

                using (var session = dest.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("Doe", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task ShouldSkipAndKeepCountersTimeSeriesAndAttachmentsOfDestinationDocument()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, skipOverwriteIfUnchanged: true, "loadToUsers({ Name: this.Name });");

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe", Age = 30 }, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                // the destination's own counters, time series and attachments end up in its @metadata
                using (var session = dest.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("likes", 5);
                    session.TimeSeriesFor("users/1", "Heartrate").Append(DateTime.UtcNow, 60);
                    session.Advanced.Attachments.Store("users/1", "photo.jpg", new MemoryStream(new byte[] { 1, 2, 3 }));
                    await session.SaveChangesAsync();
                }

                var beforeUnloadedChange = await GetDestinationStateAsync(dest);

                etlDone.Reset();
                await UpdateUserAsync(src, user => user.Age = 31);
                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                var afterUnloadedChange = await GetDestinationStateAsync(dest);
                Assert.Equal(beforeUnloadedChange.ChangeVector, afterUnloadedChange.ChangeVector);
                Assert.Equal(beforeUnloadedChange.LastDocEtag, afterUnloadedChange.LastDocEtag);

                using (var session = dest.OpenAsyncSession())
                {
                    Assert.Equal(5L, await session.CountersFor("users/1").GetAsync("likes"));
                    Assert.Single(await session.TimeSeriesFor("users/1", "Heartrate").GetAsync());
                    Assert.True(await session.Advanced.Attachments.ExistsAsync("users/1", "photo.jpg"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task ShouldWriteDataArchivalMetadataChange()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                // no script: the whole document, @metadata included, is sent
                AddEtl(src, dest, skipOverwriteIfUnchanged: true, script: null);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe", Age = 30 }, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
                var initial = await GetDestinationStateAsync(dest);

                var archiveAt = DateTime.UtcNow.AddDays(1).ToString("O");

                etlDone.Reset();
                using (var session = src.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.ArchiveAt] = archiveAt;
                    await session.SaveChangesAsync();
                }
                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                var afterArchivalChange = await GetDestinationStateAsync(dest);
                Assert.NotEqual(initial.ChangeVector, afterArchivalChange.ChangeVector);

                using (var session = dest.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(archiveAt, session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.ArchiveAt]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [InlineData("loadToPeople({ Name: this.Name });")] // different collection: delete by prefix, then put under a new id
        [InlineData("var doc = loadToUsers({ Name: this.Name }); doc.addCounter(loadCounter('likes'));")] // delete, then put
        public async Task ShouldStillWriteWhenScriptDeletesBeforePut(string script)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, skipOverwriteIfUnchanged: true, script);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe", Age = 30 }, "users/1");
                    session.CountersFor("users/1").Increment("likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
                var initialLastDocEtag = await GetLastDocEtagAsync(dest);

                etlDone.Reset();
                await UpdateUserAsync(src, user => user.Age = 31);
                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                Assert.True(await GetLastDocEtagAsync(dest) > initialLastDocEtag);
            }
        }

        private static async Task<string> GetChangeVectorAsync(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                return session.Advanced.GetChangeVectorFor(user);
            }
        }

        private static async Task<long> GetLastDocEtagAsync(IDocumentStore store)
        {
            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            return stats.LastDocEtag ?? 0;
        }

        private static async Task<string> PutUserAsync(DocumentStore store, string name, bool skipOverwriteIfUnchanged)
        {
            var document = new DynamicJsonValue
            {
                [nameof(User.Name)] = name,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Users"
                }
            };

            var requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var command = new SingleNodeBatchCommand(store.Conventions, new List<ICommandData> { new PutWithSkipFlagCommandData("users/1", document, skipOverwriteIfUnchanged) }))
            {
                await requestExecutor.ExecuteAsync(command, context);

                var reply = (BlittableJsonReaderObject)command.Result.Results[0];
                Assert.True(reply.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVector));
                return changeVector;
            }
        }

        // PutCommandData's flag is internal to the client (only Raven ETL sets it), so send the property directly
        private sealed class PutWithSkipFlagCommandData : ICommandData
        {
            private readonly DynamicJsonValue _document;
            private readonly bool _skipOverwriteIfUnchanged;

            public PutWithSkipFlagCommandData(string id, DynamicJsonValue document, bool skipOverwriteIfUnchanged)
            {
                Id = id;
                _document = document;
                _skipOverwriteIfUnchanged = skipOverwriteIfUnchanged;
            }

            public string Id { get; }
            public string Name => null;
            public string ChangeVector => null;
            public CommandType Type => CommandType.PUT;

            public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
            {
                return new DynamicJsonValue
                {
                    [nameof(Id)] = Id,
                    [nameof(ChangeVector)] = null,
                    ["Document"] = _document,
                    [nameof(Type)] = nameof(CommandType.PUT),
                    ["SkipOverwriteIfUnchanged"] = _skipOverwriteIfUnchanged
                };
            }

            public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
            {
            }
        }

        private void AddEtl(DocumentStore src, DocumentStore dest, bool skipOverwriteIfUnchanged, string script)
        {
            var connectionStringName = $"{src.Database} to {dest.Database}";

            var configuration = new RavenEtlConfiguration
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                SkipOverwriteIfUnchanged = skipOverwriteIfUnchanged,
                Transforms =
                {
                    new Transformation
                    {
                        Name = "Users",
                        Collections = { "Users" },
                        Script = script
                    }
                }
            };

            Etl.AddEtl(src, configuration, new RavenConnectionString
            {
                Name = connectionStringName,
                Database = dest.Database,
                TopologyDiscoveryUrls = dest.Urls
            });
        }

        private static async Task UpdateUserAsync(IDocumentStore store, Action<User> update)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                update(user);
                await session.SaveChangesAsync();
            }
        }

        private static async Task<(string ChangeVector, long LastDocEtag)> GetDestinationStateAsync(IDocumentStore dest)
        {
            using (var session = dest.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                var stats = await dest.Maintenance.SendAsync(new GetStatisticsOperation());
                return (session.Advanced.GetChangeVectorFor(user), stats.LastDocEtag ?? 0);
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }
    }
}

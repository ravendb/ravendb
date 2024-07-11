using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Attachments
{
    public class BulkInsertAttachments : RavenTestBase
    {
        public BulkInsertAttachments(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(1, 32 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(100, 1 * 1024 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(100, 256 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(200, 128 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(1000, 16 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        public async Task StoreManyAttachments(Options options, int count, int size)
        {
            using (var store = GetDocumentStore(options))
            {
                const string userId = "user/1";
                var streams = new Dictionary<string, MemoryStream>();
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "EGR" };
                    bulkInsert.Store(user1, userId);
                    var attachmentsBulkInsert = bulkInsert.AttachmentsFor(userId);
                    for (int i = 0; i < count; i++)
                    {
                        var rnd = new Random(DateTime.Now.Millisecond);
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
                        var name = i.ToString();
                        var stream = new MemoryStream(bArr);

                        await attachmentsBulkInsert.StoreAsync(name, stream);

                        stream.Position = 0;
                        streams[name] = stream;
                    }
                }

                var attachmentsNames = streams.Select(x => new AttachmentRequest(userId, x.Key));

                var tester = store.ForSessionTesting();

                await tester.AssertAllAsync((_, session) =>
                {
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                    while (attachmentsEnumerator.MoveNext())
                    {
                        Assert.NotNull(attachmentsEnumerator.Current != null);
                        Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[attachmentsEnumerator.Current.Details.Name]));
                    }
                });
            }
        }

        [RavenRetryTheory(RavenTestCategory.BulkInsert)]
        [RavenData(100, 100, 16 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(75, 75, 64 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        public async Task StoreManyAttachmentsAndDocs(Options options, int count, int attachments, int size)
        {
            using (var store = GetDocumentStore(options))
            {
                var streams = new Dictionary<string, Dictionary<string, MemoryStream>>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
                        var id = $"user/{i}";
                        streams[id] = new Dictionary<string, MemoryStream>();
                        bulkInsert.Store(new User { Name = $"EGR_{i}" }, id);
                        var attachmentsBulkInsert = bulkInsert.AttachmentsFor(id);
                        for (int j = 0; j < attachments; j++)
                        {
                            var rnd = new Random(DateTime.Now.Millisecond);
                            var bArr = new byte[size];
                            rnd.NextBytes(bArr);
                            var name = j.ToString();
                            var stream = new MemoryStream(bArr);
                            await attachmentsBulkInsert.StoreAsync(name, stream);

                            stream.Position = 0;
                            streams[id][name] = stream;
                        }
                    }
                }

                var tester = store.ForSessionTesting();

                foreach (var id in streams.Keys)
                {
                    await tester.AssertAllAsync((_, session) =>
                    {
                        var attachmentsNames = streams.Select(x => new AttachmentRequest(id, x.Key));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[id][attachmentsEnumerator.Current.Details.Name]));
                        }
                    });
                }
            }
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(10, 100, 32 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(500, 750, 16 * 1024, DatabaseMode = RavenDatabaseMode.All)]
        public async Task BulkStoreAttachmentsForRandomDocs(Options options, int count, int attachments, int size)
        {
            using (var store = GetDocumentStore(options))
            {
                var streams = new Dictionary<string, Dictionary<string, MemoryStream>>();
                var ids = new List<string>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
                        var id = $"user/{i}";
                        ids.Add(id);
                        streams[id] = new Dictionary<string, MemoryStream>();
                        bulkInsert.Store(new User {Name = $"EGR_{i}"}, id);
                    }

                    for (int j = 0; j < attachments; j++)
                    {
                        var rnd = new Random(DateTime.Now.Millisecond);
                        var id = ids[rnd.Next(0, count)];
                        var attachmentsBulkInsert = bulkInsert.AttachmentsFor(id);
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
                        var name = j.ToString();
                        var stream = new MemoryStream(bArr);
                        await attachmentsBulkInsert.StoreAsync(name, stream);

                        stream.Position = 0;
                        streams[id][name] = stream;
                    }
                }
                
                var tester = store.ForSessionTesting();

                foreach (var id in streams.Keys)
                {
                    await tester.AssertAllAsync((_, session) =>
                    {
                        var attachmentsNames = streams.Select(x => new AttachmentRequest(id, x.Key));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[id][attachmentsEnumerator.Current.Details.Name]));
                        }
                    });
                }
            }
        }


        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanHaveAttachmentBulkInsertsWithCounters(Options options)
        {
            int count = 100;
            int size = 64 * 1024;
            using (var store = GetDocumentStore(options))
            {
                var streams = new Dictionary<string, Dictionary<string, MemoryStream>>();
                var counters = new Dictionary<string, string>();
                var bulks = new Dictionary<string, BulkInsertOperation.AttachmentsBulkInsert>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
                        var id = $"user/{i}";
                        streams[id] = new Dictionary<string, MemoryStream>();
                        bulkInsert.Store(new User { Name = $"EGR_{i}" }, id);
                        bulks[id] = bulkInsert.AttachmentsFor(id);
                    }

                    foreach (var bulk in bulks)
                    {
                        var rnd = new Random(DateTime.Now.Millisecond);
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
                        var name = $"{bulk.Key}_{rnd.Next(100)}";
                        var stream = new MemoryStream(bArr);
                        await bulk.Value.StoreAsync(name, stream);

                        stream.Position = 0;
                        streams[bulk.Key][name] = stream;
                        await bulkInsert.CountersFor(bulk.Key).IncrementAsync(name);
                        counters[bulk.Key] = name;
                    }
                }

                var tester = store.ForSessionTesting();

                foreach (var id in streams.Keys)
                {
                    await tester.AssertAllAsync((_, session) =>
                    {
                        var attachmentsNames = streams.Select(x => new AttachmentRequest(id, x.Key));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[id][attachmentsEnumerator.Current.Details.Name]));
                        }
                    });

                    var val = store.Operations
                        .Send(new GetCountersOperation(id, new[] { counters[id] }))
                        .Counters[0]?.TotalValue;
                    Assert.Equal(1, val);
                }
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public void StoreAsyncShouldThrowIfRunningTimeSeriesBulkInsert()
        {
            using (var store = GetDocumentStore())
            {
                var argumentError = Assert.Throws<BulkInsertInvalidOperationException> (() =>
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        bulkInsert.TimeSeriesFor("id", "name");
                        var bulk = bulkInsert.AttachmentsFor("id");
                        bulk.Store("name", new MemoryStream());
                    }
                });

                Assert.Equal("There is an already running time series operation, did you forget to Dispose it?", argumentError.Message);
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public void StoreAsyncNullId()
        {
            using (var store = GetDocumentStore())
            {
                var argumentError = Assert.Throws<ArgumentException>(() =>
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        bulkInsert.AttachmentsFor(null);
                    }
                });

                Assert.Equal("Document id cannot be null or empty (Parameter 'id')", argumentError.Message);
            }
        }

        // RavenDB-14934
        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldUpdateDocumentChangeAfterInsertingAttachment(Options options)
        {
            int count = 10;
            int attachments = 10;
            int size = 16 * 1024;
            using (var store = GetDocumentStore(options))
            {
                var streams = new Dictionary<string, Dictionary<string, MemoryStream>>();
                var changeVectorsBefore = new Dictionary<string, string>();
                for (int i = 0; i < count; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var id = $"user/{i}";
                        var u = new User { Name = $"EGR_{i}" };
                        await session.StoreAsync(u, id);
                        await session.SaveChangesAsync();
                        var changeVector = session.Advanced.GetChangeVectorFor(u);
                        changeVectorsBefore.Add(id, changeVector);
                        streams[id] = new Dictionary<string, MemoryStream>();
                    }
                }

                var changeVectorsAfter = new Dictionary<string, string>();
                using (var bulkInsert = store.BulkInsert())
                {
                    foreach (var id in streams.Keys)
                    {
                        var attachmentsBulkInsert = bulkInsert.AttachmentsFor(id);
                        for (int j = 0; j < attachments; j++)
                        {
                            var rnd = new Random(DateTime.Now.Millisecond);
                            var bArr = new byte[size];
                            rnd.NextBytes(bArr);
                            var name = j.ToString();
                            var stream = new MemoryStream(bArr);
                            await attachmentsBulkInsert.StoreAsync(name, stream);

                            stream.Position = 0;
                            streams[id][name] = stream;
                        }
                    }
                }

                var tester = store.ForSessionTesting();

                foreach (var id in streams.Keys)
                {
                    await tester.AssertAllAsync((_, session) =>
                    {
                        var attachmentsNames = streams.Select(x => new AttachmentRequest(id, x.Key));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[id][attachmentsEnumerator.Current.Details.Name]));
                        }
                    });

                    using (var session = store.OpenSession())
                    {
                        var u = session.Load<User>(id);
                        var changeVector = session.Advanced.GetChangeVectorFor(u);
                        changeVectorsAfter.Add(id, changeVector);
                    }
                }

                Assert.All(changeVectorsBefore, x => changeVectorsAfter.ContainsKey(x.Key));

                foreach (var kvp in changeVectorsBefore)
                {
                    Assert.Contains(kvp.Key, changeVectorsAfter.Keys);
                    var cvStatus = ChangeVectorUtils.GetConflictStatus(changeVectorsAfter[kvp.Key], kvp.Value);
                    Assert.Equal(ConflictStatus.Update, cvStatus);
                }
            }
        }
    }
}

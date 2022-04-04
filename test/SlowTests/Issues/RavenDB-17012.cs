using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Tests.Infrastructure.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17012 : RavenTestBase
    {
        public RavenDB_17012(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(10, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(500, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(5_000, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_SkipOverwriteIfUnchanged(Options options, int docsCount)
        {
            using (IDocumentStore store = GetDocumentStore(options))
            {
                var docs = new List<User>();

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < docsCount; i++)
                    {
                        var user = new User { Age = i };
                        docs.Add(user);
                        await bulk.StoreAsync(user, i.ToString());
                    }
                }

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                var lastEtags = new Dictionary<UniqueDatabaseInstanceKey, long?>();

                await tester.AssertAllAsync((key, stats) =>
                {
                    lastEtags[key] = stats.LastDocEtag;
                });

                using (var bulk = store.BulkInsert(new BulkInsertOptions
                {
                    SkipOverwriteIfUnchanged = true
                }))
                {
                    for (var i = 0; i < docsCount; i++)
                    {
                        var doc = docs[i];
                        await bulk.StoreAsync(doc, i.ToString());
                    }
                }

                await tester.AssertAllAsync((key, stats) =>
                {
                    Assert.Equal(lastEtags[key], stats.LastDocEtag);
                });
            }
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(10, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(500, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(5_000, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_SkipOverwriteIfUnchanged_SomeDocuments(Options options, int docsCount)
        {
            using (IDocumentStore store = GetDocumentStore(options))
            {
                var docs = new List<User>();

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < docsCount; i++)
                    {
                        var user = new User { Age = i };
                        docs.Add(user);
                        await bulk.StoreAsync(user, i.ToString());
                    }
                }

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                var lastEtags = new Dictionary<UniqueDatabaseInstanceKey, long?>();

                await tester.AssertAllAsync((key, stats) =>
                {
                    lastEtags[key] = stats.LastDocEtag;
                });
                
                using (var bulk = store.BulkInsert(new BulkInsertOptions
                {
                    SkipOverwriteIfUnchanged = true
                }))
                {
                    for (var i = 0; i < docsCount; i++)
                    {
                        var doc = docs[i];
                        if (i % 2 == 0)
                            doc.Age = (i + 1) * 2;

                        await bulk.StoreAsync(doc, i.ToString());
                    }
                }

                tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                await tester.AssertAllAsync((key, stats) =>
                {
                    if (options.DatabaseMode == RavenDatabaseMode.Single)
                    {
                        Assert.Equal(lastEtags[key] + docsCount / 2, stats.LastDocEtag);
                    }
                    else
                    {
                        Assert.True(stats.LastDocEtag < lastEtags[key] + docsCount / 2);
                    }
                });
            }
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_SkipOverwriteIfUnchanged_With_Attachment(Options options)
        {
            using (IDocumentStore store = GetDocumentStore(options))
            {
                var docId = Guid.NewGuid().ToString();
                var attachmentName = Guid.NewGuid().ToString();

                using (var bulk = store.BulkInsert())
                {
                    var user = new User { Name = docId };
                    await bulk.StoreAsync(user, docId);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Attachments.Store(docId, attachmentName, new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                    await session.SaveChangesAsync();
                }

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                var lastEtags = new Dictionary<UniqueDatabaseInstanceKey, long?>();

                await tester.AssertAllAsync((key, stats) =>
                {
                    lastEtags[key] = stats.LastDocEtag;
                });

                using (var bulk = store.BulkInsert(new BulkInsertOptions
                {
                    SkipOverwriteIfUnchanged = true
                }))
                {
                    var user = new User { Name = docId };
                    await bulk.StoreAsync(user, docId);
                }

                await tester.AssertAllAsync((key, stats) =>
                {
                    Assert.Equal(lastEtags[key], stats.LastDocEtag);
                });

                var sessionTester = store.ForSessionTesting();

                await sessionTester.AssertOneAsync((_, session) =>
                {
                    using (session)
                    {
                        var attachment = session.Advanced.Attachments.Get(docId, attachmentName);
                        Assert.NotNull(attachment);
                    }
                });
            }
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_SkipOverwriteIfUnchanged_With_Counter(Options options)
        {
            using (IDocumentStore store = GetDocumentStore(options))
            {
                var docId = Guid.NewGuid().ToString();
                var counterName = Guid.NewGuid().ToString();

                using (var bulk = store.BulkInsert())
                {
                    var user = new User { Name = docId };
                    await bulk.StoreAsync(user, docId);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor(docId).Increment(counterName, 1);
                    await session.SaveChangesAsync();
                }

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                var lastEtags = new Dictionary<UniqueDatabaseInstanceKey, long?>();

                await tester.AssertAllAsync((key, stats) =>
                {
                    lastEtags[key] = stats.LastDocEtag;
                });

                using (var bulk = store.BulkInsert(new BulkInsertOptions
                {
                    SkipOverwriteIfUnchanged = true
                }))
                {
                    var user = new User { Name = docId };
                    await bulk.StoreAsync(user, docId);
                }

                await tester.AssertAllAsync((key, stats) =>
                {
                    Assert.Equal(lastEtags[key], stats.LastDocEtag);
                });

                var sessionTester = store.ForSessionTesting();

                await sessionTester.AssertOneAsync((_, session) =>
                {
                    using (session)
                    {
                        var counter = session.CountersFor(docId).Get(counterName);
                        Assert.Equal(1, counter);
                    }
                });
            }
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_SkipOverwriteIfUnchanged_With_TimeSeries(Options options)
        {
            using (IDocumentStore store = GetDocumentStore(options))
            {
                var docId = Guid.NewGuid().ToString();
                var timeSeriesName = Guid.NewGuid().ToString();

                using (var bulk = store.BulkInsert())
                {
                    var user = new User { Name = docId };
                    await bulk.StoreAsync(user, docId);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor(docId, timeSeriesName).Append(DateTime.Now, 1);
                    await session.SaveChangesAsync();
                }

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                var lastEtags = new Dictionary<UniqueDatabaseInstanceKey, long?>();

                await tester.AssertAllAsync((key, stats) =>
                {
                    lastEtags[key] = stats.LastDocEtag;
                });

                using (var bulk = store.BulkInsert(new BulkInsertOptions
                {
                    SkipOverwriteIfUnchanged = true
                }))
                {
                    var user = new User { Name = docId };
                    await bulk.StoreAsync(user, docId);
                }

                await tester.AssertAllAsync((key, stats) =>
                {
                    Assert.Equal(lastEtags[key], stats.LastDocEtag);
                });

                var sessionTester = store.ForSessionTesting();

                await sessionTester.AssertOneAsync((_, session) =>
                {
                    using (session)
                    {
                        var entries = session.TimeSeriesFor(docId, timeSeriesName).Get();

                        if (entries != null)
                            Assert.Equal(1, entries.Length);
                    }
                });
            }
        }
    }
}

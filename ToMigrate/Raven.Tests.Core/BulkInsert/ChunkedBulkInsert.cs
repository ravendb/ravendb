using Raven.Abstractions.Data;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Raven.Tests.Core.ChangesApi;
using Xunit;

namespace Raven.Tests.Core.BulkInsert
{
    public class ChunkedBulkInsert : RavenCoreTestBase
    {
#if DNXCORE50
        public ChunkedBulkInsert(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        public class Node
        {
            public string Name { get; set; }
            public Node[] Children { get; set; }
        }

        [Fact]
        public void ChunkVolumeConstraint()
        {
            var bulkInsertStartsCounter = 0;
            using (var store = GetDocumentStore())
            {
                store.Changes().Task.Result.ForBulkInsert().Task.Result
                    .Subscribe(new ActionObserver<BulkInsertChangeNotification>(x =>
                    {
                        if (x.Type == DocumentChangeTypes.BulkInsertStarted)
                            Interlocked.Increment(ref bulkInsertStartsCounter);
                    }));

                using (var bulkInsert = store.BulkInsert(options: new BulkInsertOptions
                {
                    ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
                    {
                        MaxChunkVolumeInBytes = 5
                    }
                }))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        bulkInsert.Store(new Node
                        {
                            Name = "Parent",
                            Children = Enumerable.Range(0, 5).Select(x => new Node { Name = "Child" + x }).ToArray()
                        });
                    }
                }

                Assert.Equal(20, bulkInsertStartsCounter);
                using (var session = store.OpenSession())
                {
                    var count = session.Query<Node>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(20, count);
                }
            }

        }

        [Fact]
        public void ChunkVolumeConstraintMakeSureUnneededConnectionsNotCreated()
        {
            var bulkInsertStartsCounter = 0;
            using (var store = GetDocumentStore())
            {
                store.Changes().Task.Result.ForBulkInsert().Task.Result
                    .Subscribe(new ActionObserver<BulkInsertChangeNotification>(x =>
                    {
                        if (x.Type == DocumentChangeTypes.BulkInsertStarted)
                            Interlocked.Increment(ref bulkInsertStartsCounter);
                    }));
                
                using (var bulkInsert = store.BulkInsert(options: new Abstractions.Data.BulkInsertOptions
                {
                    ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
                    {
                        MaxChunkVolumeInBytes = 10 * 1024
                    }
                }))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        bulkInsert.Store(new Node
                        {
                            Name = "Parent",
                            Children = Enumerable.Range(0, 5).Select(x => new Node { Name = "Child" + x }).ToArray()
                        });
                    }
                }

                Assert.Equal(1, bulkInsertStartsCounter);
                using (var session = store.OpenSession())
                {
                    var count = session.Query<Node>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(20, count);
                }
            }

        }

        [Fact]
        public void DocumentsInChunkConstraint()
        {
            var bulkInsertStartsCounter = 0;
            using (var store = GetDocumentStore())
            {
                store.Changes().Task.Result.ForBulkInsert().Task.Result
                     .Subscribe(new ActionObserver<BulkInsertChangeNotification>(x =>
                     {
                         if (x.Type == DocumentChangeTypes.BulkInsertStarted)
                             Interlocked.Increment(ref bulkInsertStartsCounter);
                     }));
                
                using (var bulkInsert = store.BulkInsert(options: new Abstractions.Data.BulkInsertOptions
                {
                    ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
                    {
                        MaxDocumentsPerChunk = 1
                    }
                }))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        bulkInsert.Store(new Node
                        {
                            Name = "Parent",
                        });
                    }
                }

                Assert.Equal(20, bulkInsertStartsCounter);
                using (var session = store.OpenSession())
                {
                    var count = session.Query<Node>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(count, 20);
                }
            }

        }

        [Fact]
        public void DocumentsInChunkConstraintMakeSureUnneedConnectionsNotCreated()
        {
            var bulkInsertStartsCounter = 0;
            var mre = new ManualResetEventSlim();
            using (var store = GetDocumentStore())
            {
                store.Changes().Task.Result.ForBulkInsert().Task.Result
                     .Subscribe(new ActionObserver<BulkInsertChangeNotification>(x =>
                     {
                         if (x.Type == DocumentChangeTypes.BulkInsertStarted)
                         {
                             Interlocked.Increment(ref bulkInsertStartsCounter);
                             mre.Set();
                         }
                     }));

                using (var bulkInsert = store.BulkInsert(options: new Abstractions.Data.BulkInsertOptions
                {
                    ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
                    {
                        MaxDocumentsPerChunk = 20
                    }
                }))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        bulkInsert.Store(new Node
                        {
                            Name = "Parent",
                        });
                    }
                }
                mre.Wait(1000);
                Assert.Equal(1, bulkInsertStartsCounter);
                using (var session = store.OpenSession())
                {
                    var count = session.Query<Node>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(count, 20);
                }
            }
        }

        [Fact]
        public void ValidateChunkedBulkInsertOperationsIDsCount()
        {
            var bulkInsertStartsCounter = new ConcurrentDictionary<Guid, BulkInsertChangeNotification>();
            using (var store = GetDocumentStore())
            {
                store.Changes().Task.Result.ForBulkInsert().Task.Result
                    .Subscribe(new ActionObserver<BulkInsertChangeNotification>(x =>
                    {
                        if (x.Type == DocumentChangeTypes.BulkInsertStarted)
                            Assert.True(bulkInsertStartsCounter.TryAdd(x.OperationId, x));
                    }));
                
                for (var i = 0; i < 10; i++)
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        bulkInsert.Store(new Node
                        {
                            Name = "Parent",
                            Children = Enumerable.Range(0, 5).Select(x => new Node { Name = "Child" + x }).ToArray()
                        });
                    }
                }
            }

            Assert.Equal(10, bulkInsertStartsCounter.Count);
        }
    }
}
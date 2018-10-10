using Raven.Abstractions.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client.Document;
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
            using (var store = GetDocumentStore())
            {
                var bulkInsertStartsCounter = 0;
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
                    bulkInsertStartsCounter = ((ChunkedRemoteBulkInsertOperation) bulkInsert.Operation).RemoteBulkInsertOperationSwitches;
                }

                Assert.Equal(20, bulkInsertStartsCounter);
                using (var session = store.OpenSession())
                {
                    var count = session.Query<Node>().Customize(x => x.WaitForNonStaleResults()).Count();
                    if(20 != count)
                        WaitForUserToContinueTheTest(store);
                }
            }

        }

        [Fact]
        public void ChunkVolumeConstraintMakeSureUnneededConnectionsNotCreated()
        {
            using (var store = GetDocumentStore())
            {
                var bulkInsertStartsCounter = 0;
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
                    bulkInsertStartsCounter = ((ChunkedRemoteBulkInsertOperation) bulkInsert.Operation).RemoteBulkInsertOperationSwitches;
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
            using (var store = GetDocumentStore())
            {
                var remoteBulkInsertOperationSwitches = 0;
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

                    remoteBulkInsertOperationSwitches = ((ChunkedRemoteBulkInsertOperation) bulkInsert.Operation).RemoteBulkInsertOperationSwitches;
                }

                Assert.Equal(20, remoteBulkInsertOperationSwitches);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Node>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(20,count);
                }
            }
        }

        [Fact]
        public void DocumentsInChunkConstraintMakeSureUnneedConnectionsNotCreated()
        {
            var mre = new ManualResetEventSlim();
            using (var store = GetDocumentStore())
            {
                var bulkInsertStartsCounter = 0;
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

                    bulkInsertStartsCounter = ((ChunkedRemoteBulkInsertOperation) bulkInsert.Operation).RemoteBulkInsertOperationSwitches;
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
            var bulkInsertStartsCounter = new HashSet<Guid>();
            using (var store = GetDocumentStore())
            {
                for (var i = 0; i < 10; i++)
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        bulkInsert.Store(new Node
                        {
                            Name = "Parent",
                            Children = Enumerable.Range(0, 5).Select(x => new Node { Name = "Child" + x }).ToArray()
                        });

                        bulkInsertStartsCounter.Add(bulkInsert.OperationId);
                    }
                }
            }

            Assert.Equal(10, bulkInsertStartsCounter.Count);
        }
    }
}
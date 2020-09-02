using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Revisions;
using FastTests.Utils;
using FastTests.Voron.Util;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server
{
    public class RecordingTransactionOperationsMergerTests : ClusterTestBase
    {
        public RecordingTransactionOperationsMergerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AllDerivedCommandsOfMergedTransactionCommand_MustBeRecordable_ExceptForExceptions()
        {
            var exceptions = new[]
            {
                typeof(TransactionOperationsMerger.MergedTransactionCommand),
                typeof(ExecuteRateLimitedOperations<>),
                typeof(StartTransactionsRecordingCommand),
                typeof(StopTransactionsRecordingCommand),
                typeof(BatchHandler.TransactionMergedCommand),
                typeof(AbstractQueryRunner.BulkOperationCommand<>)
            };

            var commandBaseType = typeof(TransactionOperationsMerger.MergedTransactionCommand);
            var types = commandBaseType.Assembly.GetTypes();
            var commandDeriveTypes = types
                .Where(t => commandBaseType.IsAbstract == false && commandBaseType.IsAssignableFrom(t) && exceptions.Contains(t) == false)
                .ToList();

            var iRecordableType = typeof(TransactionOperationsMerger.IReplayableCommandDto<>);
            var genericTypes = iRecordableType.Assembly.GetTypes()
                .Select(t => t
                    .GetInterfaces()
                    .Where(i => i.IsGenericType)
                    .FirstOrDefault(i => iRecordableType.IsAssignableFrom(i.GetGenericTypeDefinition())))
                .Where(t => t != null)
                .Select(t => t.GetGenericArguments()[0]);

            Assert.All(commandDeriveTypes, dt =>
            {
                Assert.True(genericTypes.Contains(dt), $"{dt.Name} should has equivalent dto - {dt.Name}Dto : {iRecordableType.Name}");
            });
        }

        [Fact]
        public void Replay_WhenCancelInTheMiddle_ShouldKeepAcceptWriteOperation()
        {
            var filePath = NewDataPath();

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                for (var i = 0; i < 500; i++)
                {

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = $"User{i}" });

                        session.SaveChanges();
                    }
                }
                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);

                Task.Run(() =>
                {
                    try
                    {
                        store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                });

                var expected = new User { Name = "FinalUser" };
                using (var session = store.OpenSession())
                {
                    var stop = Stopwatch.StartNew();
                    while (session.Query<User>().Any() == false)
                    {
                        if (stop.Elapsed > TimeSpan.FromSeconds(5))
                        {
                            store.Commands().Execute(new KillOperationCommand(command.Result));
                            throw new TimeoutException();
                        }
                        Thread.Sleep(50);
                    }
                    store.Commands().Execute(new KillOperationCommand(command.Result));

                    session.Store(expected);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var actual = session.Load<User>(expected.Id);
                    Assert.Equal(expected.Name, actual.Name);
                }
            }
        }

        [Fact]
        public void RecordingClusterTransactionMergedCommand()
        {

            var recordFilePath = NewDataPath();

            var onlyStored = new User { Name = "Andrea" };
            var deleted = new User { Name = "Brooke" };

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Store(onlyStored);
                    session.Store(deleted);
                    session.SaveChanges();

                    session.Delete(deleted);
                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                //Assert
                using (var session = store.OpenSession())
                {
                    var actualUser = session.Load<User>(onlyStored.Id);
                    Assert.Equal(onlyStored, actualUser, new UserComparer());
                    deleted = session.Load<User>(deleted.Id);
                    Assert.Null(deleted);
                }
            }
        }

        [Fact]
        public void RecordingExecuteCounterBatchCommand()
        {
            var recordFilePath = NewDataPath();

            var user = new User { Name = "August" };

            const string id = "users/A-1";
            //Recording
            const string incrementedCounter = "likes";
            const string deletedCounter = "Anger";
            const string putCounter = "Sadness";
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                using (var session = store.OpenSession())
                {
                    session.Store(user, id);
                    session.SaveChanges();
                }

                var counterOperations = new List<CounterOperation>
                {
                    new CounterOperation
                    {
                        Type = CounterOperationType.Increment,
                        CounterName = incrementedCounter,
                        Delta = 0
                    },
                    new CounterOperation
                    {
                        Type = CounterOperationType.Increment,
                        CounterName = deletedCounter
                    },
                    new CounterOperation
                    {
                        Type = CounterOperationType.Increment,
                        CounterName = putCounter,
                        Delta = 30,
                    }
                };
                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = id,
                            Operations = counterOperations
                        }
                    }
                }));

                var counterOperations2 = new List<CounterOperation>
                {
                    new CounterOperation
                    {
                        Type = CounterOperationType.Delete,
                        CounterName = deletedCounter
                    },
                };
                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = id,
                            Operations = counterOperations2
                        }
                    }
                }));

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var result = store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = id,
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Get,
                                    CounterName = incrementedCounter
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Get,
                                    CounterName = deletedCounter
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Get,
                                    CounterName = putCounter
                                },
                            }
                        }
                    }
                }));

                var actualCounters = result.Counters.Select(c => c?.CounterName).ToArray();
                Assert.Contains(incrementedCounter, actualCounters);
                Assert.Contains(putCounter, actualCounters);
                Assert.Contains(null, actualCounters); // deleted Counter
            }
        }

        [Fact]
        public async Task ReplayOutputReduceToCollectionCommand()
        {
            var recordFilePath = NewDataPath();

            var expected = new[]
            {
                new AgeGroup{Age = 120, Amount = 2},
                new AgeGroup{Age = 24, Amount = 2},
            };

            var users = new[]
            {
                new User{Name = "Andre", Age = 120},
                new User{Name = "Barbara", Age = 120},
                new User{Name = "Charlotte", Age = 24},
                new User{Name = "Dominic", Age = 24},
            };

            using (var store = GetDocumentStore())
            {
                //Recording
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                using (var session = store.OpenSession())
                {
                    foreach (var user in users)
                    {
                        session.Store(user);
                    }

                    session.SaveChanges();
                }

                await store.ExecuteIndexAsync(new DailyInvoicesIndex());

                WaitForIndexing(store);

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var actual = session.Query<AgeGroup>(null, nameof(AgeGroup)).ToList();

                    //Assert
                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(expected.Length, actual.Count);
                    Assert.All(expected, g => Assert.True(actual.Contains(g), $"Expected {g}"));
                }
            }
        }

        public class DailyInvoicesIndex : AbstractIndexCreationTask<User, AgeGroup>
        {
            public DailyInvoicesIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new AgeGroup
                    {
                        Age = invoice.Age,
                        Amount = 1
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Age
                    into g
                    select new AgeGroup
                    {
                        Age = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "AgeGroup";
            }
        }

        public class AgeGroup
        {
            public int Age { get; set; }
            public int Amount { get; set; }

            public override bool Equals(object otherObj)
            {
                return otherObj is AgeGroup other &&
                    Age == other.Age &&
                    Amount == other.Amount;
            }

            public override int GetHashCode()
            {
                return 1;
            }

            public override string ToString()
            {
                return $"AgeGroup: Age({Age}) Amount({Amount})";
            }
        }

        [Fact]
        public async Task ReplayDeleteTombstonesCommand()
        {
            var recordFilePath = NewDataPath();

            const string id = "UsersA-1";
            var expected = new User { Name = "Avi" };

            using (var store = GetDocumentStore())
            {
                //Recording
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                using (var session = store.OpenSession())
                {
                    session.Store(expected, null, id);
                    session.SaveChanges();
                }

                store.Commands().Delete(id, null);

                //Wait for all tombstones to exhaust their purpose
                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    await WaitFor(() =>
                    {
                        database.TombstoneCleaner.ExecuteCleanup().GetAwaiter().GetResult();
                        using (context.OpenReadTransaction())
                        {
                            var tombstones = database.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();
                            return !tombstones.Any();
                        }
                    });

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = database.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    //Assert
                    Assert.Empty(tombstones);
                }
            }
        }

        [Fact]
        public async Task RecordingDeleteRevisionsBeforeCommand()
        {
            var recordFilePath = NewDataPath();

            const string id = "UsersA-1";
            var user = new User { Name = "Andre" };

            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                });

                //Recording
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                using (var session = store.OpenSession())
                {
                    session.Store(user, id);
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.DocumentsStorage.RevisionsStorage.Operations.DeleteRevisionsBefore("Users", DateTime.UtcNow);

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                });

                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);

                var error = Assert.Throws<RavenException>(() =>
                {
                    store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));
                });

                Assert.IsType<InvalidOperationException>(error.InnerException);
            }
        }

        [Fact]
        public async Task RecordingDeleteRevisionsCommand()
        {
            var recordFilePath = NewDataPath();

            const string id = "UsersA-1";
            var user = new User { Name = "Andre" };

            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                });

                //Recording
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                store.Commands().Put(id, null, user);

                store.Maintenance.Send(new RevisionsTests.DeleteRevisionsOperation(new AdminRevisionsHandler.Parameters
                {
                    DocumentIds = new[] { id }
                }));

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                });

                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var revisions = session.Advanced.Revisions.GetFor<User>(id);
                    //Assert
                    Assert.Empty(revisions);
                }
            }
        }
        [Fact]
        public async Task RecordingUpdateSiblingCurrentEtag()
        {
            var recordFilePath = NewDataPath();

            const int expectedEtag = 5;
            var senderDatabaseId = Guid.NewGuid().ToString();

            using (var store = GetDocumentStore())
            {

                //Recording
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                var message = new ReplicationMessageReply
                {
                    Type = ReplicationMessageReply.ReplyType.Ok,
                    MessageType = "Heartbeat",
                    NodeTag = "C",
                    CurrentEtag = expectedEtag,
                    DatabaseId = senderDatabaseId,
                };

                var command = new OutgoingReplicationHandler.UpdateSiblingCurrentEtag(message, new AsyncManualResetEvent());
                command.Init();

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TxMerger.Enqueue(command);

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    //Assert
                    var actualEtag = DocumentsStorage.GetLastReplicatedEtagFrom(context, senderDatabaseId);
                    Assert.Equal(expectedEtag, actualEtag);
                }
            }
        }

        [Fact]
        public async Task RecordingMergedUpdateDatabaseChangeVectorCommand()
        {
            var recordFilePath = NewDataPath();

            var expectedChangeVector = "A:1-r8gQ6pvUAEyEWKO64hoG5A";

            using (var store = GetDocumentStore())
            {

                //Recording
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                var command = new IncomingReplicationHandler.MergedUpdateDatabaseChangeVectorCommand(
                    expectedChangeVector, 5, Guid.NewGuid().ToString(), new AsyncManualResetEvent(), isHub: false);

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TxMerger.Enqueue(command);

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    //Assert
                    var actualChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                    Assert.Equal(expectedChangeVector, actualChangeVector);
                }
            }
        }

        [Fact]
        public async Task RecordingDeleteExpiredDocumentsCommand()
        {
            var recordFilePath = NewDataPath();

            const string id = "UsersA-1";
            var user = new User { Name = "Avi" };

            using (var store = GetDocumentStore())
            {
                //Recording
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, new ExpirationConfiguration
                {
                    Disabled = false,
                    DeleteFrequencyInSec = 1
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user, id);

                    var metadata = session.Advanced.GetMetadataFor(user);
                    var expiry = DateTime.UtcNow.AddSeconds(1);
                    metadata[Constants.Documents.Metadata.Expires] = expiry;

                    await session.SaveChangesAsync();
                }

                await WaitFor(() => null == store.Commands().Get(id));

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                //Assert
                Assert.Null(store.Commands().Get(id));
            }
        }

        [Fact]
        public async Task WaitForReplayTransactionsRecordingOperation()
        {
            var recordFilePath = NewDataPath();

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                for (var i = 0; i < 1000; i++)
                {
                    await store.Commands().PutAsync("user/", null, new User { Name = $"someName{i}" });
                }

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);

                var task = Task.Run(() => { store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result)); });

                var operation = new Operation(store.Commands().RequestExecutor, () => store.Changes(), store.Conventions, command.Result);
                var operationProgresses = new List<IOperationProgress>();
                operation.OnProgressChanged += p => operationProgresses.Add(p);

                await task; // we need to wait for the task
                            // before executing WaitForCompletionAsync here
                            // to avoid a race condition where we will wait for an operation
                            // before it starts (request is send to the server)

                var result = await operation.WaitForCompletionAsync<ReplayTxOperationResult>();

                //Assert
                //Todo To think how to assert this test and if this test should be exist
            }
        }

        [Fact]
        public async Task RecordingMergedDocumentReplicationCommand_WithAttachment()
        {
            var recordFilePath = NewDataPath();

            const string id = "UsersA-1";
            var user = new User
            {
                Name = "Avi"
            };

            const string bufferContent = "Menahem";
            var expected = Encoding.ASCII.GetBytes(bufferContent);
            var attachmentStream = new MemoryStream(expected);

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                var databaseWatcher = new ExternalReplication(slave.Database, $"ConnectionString-{slave.Identifier}");

                var result = await master.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = databaseWatcher.ConnectionStringName,
                    Database = databaseWatcher.Database,
                    TopologyDiscoveryUrls = master.Urls
                }));
                Assert.NotNull(result.RaftCommandIndex);

                await master.Maintenance.SendAsync(new UpdateExternalReplicationOperation(databaseWatcher));

                //Recording
                slave.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                master.Commands().Put(id, null, user);
                await WaitFor(() =>
                    null != slave.Commands().Get(id));


                const string attachmentName = "someAttachmentName";
                master.Operations.Send(new PutAttachmentOperation(id, attachmentName, attachmentStream));
                await WaitFor(() =>
                    null != slave.Operations.Send(new GetAttachmentOperation(id, attachmentName, AttachmentType.Document, null)));


                slave.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var attachmentResult = store.Operations.Send(new GetAttachmentOperation(id, "someAttachmentName", AttachmentType.Document, null));

                //Assert
                Assert.Equal(expected, attachmentResult.Stream.ReadData());
            }
        }

        private static async Task WaitFor(Func<bool> canContinue)
        {
            const int maxSecondToWait = 15;

            var startTime = DateTime.Now;
            while (true)
            {
                if ((DateTime.Now - startTime).Seconds > maxSecondToWait)
                {
                    throw new TimeoutException($"The replication takes more than {maxSecondToWait} seconds.");
                }
                if (canContinue())
                {
                    break;
                }
                await Task.Delay(100);
            }
        }

        [Fact]
        public async Task RecordingMergedDocumentReplicationCommand()
        {
            var recordFilePath = NewDataPath();

            const string id = "UsersA-1";
            var expected = new User
            {
                Name = "Avi"
            };

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                var databaseWatcher = new ExternalReplication(slave.Database, $"ConnectionString-{slave.Identifier}");

                var result = await master.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = databaseWatcher.ConnectionStringName,
                    Database = databaseWatcher.Database,
                    TopologyDiscoveryUrls = master.Urls
                }));
                Assert.NotNull(result.RaftCommandIndex);
                await master.Maintenance.SendAsync(new UpdateExternalReplicationOperation(databaseWatcher));

                //Recording
                slave.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                master.Commands().Put(id, null, expected);
                await WaitFor(() =>
                    null != slave.Commands().Get(id));

                slave.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var actual = session.Load<User>(id);

                    //Assert
                    Assert.Equal(expected, actual, new UserComparer());
                }
            }
        }

        [Fact]
        public async Task RecordingMergedHiLoReturnCommand()
        {
            var filePath = NewDataPath();
            var user = new User();

            string expected;
            using (var firstStore = GetDocumentStore())
            {
                var secondStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = firstStore.Database
                };
                secondStore.Initialize();
                await secondStore.Conventions.AsyncDocumentIdGenerator(secondStore.Database, user);

                //Recording
                firstStore.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                secondStore.Dispose();
                firstStore.Maintenance.Send(new StopTransactionsRecordingOperation());

                expected = await secondStore.Conventions.AsyncDocumentIdGenerator(firstStore.Database, user);
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                await store.Conventions.AsyncDocumentIdGenerator(store.Database, user);

                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var actual = await store.Conventions.AsyncDocumentIdGenerator(store.Database, user);
                //Assert

                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public async Task RecordingMergedNextHiLoCommand()
        {
            var filePath = NewDataPath();
            var user = new User();

            //Recording
            string expected;
            using (var store1 = GetDocumentStore())
            using (var store2 = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = store1.Database
            })
            {
                store2.Initialize();

                store1.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                await store1.Conventions.AsyncDocumentIdGenerator(store1.Database, user);
                store1.Maintenance.Send(new StopTransactionsRecordingOperation());

                expected = await store2.Conventions.AsyncDocumentIdGenerator(store1.Database, user);
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var actual = await store.Conventions.AsyncDocumentIdGenerator(store.Database, user);
                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public async Task RecordingPutResolvedConflictsCommand()
        {
            var recordFilePath = NewDataPath();

            const string id = "Users\rA-1";
            var expected = new User { Name = "Avi" };

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                slave.Maintenance.Server.Send(new ModifyConflictSolverOperation(slave.Database));

                var databaseWatcher = new ExternalReplication(slave.Database, $"ConnectionString-{slave.Identifier}");

                var result = await master.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = databaseWatcher.ConnectionStringName,
                    Database = databaseWatcher.Database,
                    TopologyDiscoveryUrls = master.Urls
                }));
                Assert.NotNull(result.RaftCommandIndex);

                //Recording
                slave.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                await master.Maintenance.SendAsync(new UpdateExternalReplicationOperation(databaseWatcher));

                slave.Commands().Put(id, null, expected);

                expected.Age = 67;
                master.Commands().Put(id, null, expected);

                await WaitForConflict(slave, id);

                slave.Maintenance.Server.Send(new ModifyConflictSolverOperation(slave.Database, null, true));

                await WaitForConflictToBeResolved(slave, id);

                slave.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                store.Maintenance.Server.Send(new ModifyConflictSolverOperation(store.Database));

                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var actual = session.Load<User>(id);

                    //Assert
                    Assert.Equal(expected, actual, new UserComparer());
                }
            }
        }

        private static async Task WaitForConflictToBeResolved(IDocumentStore slave, string id, int timeout = 15_000)
        {
            var timeoutAsTimeSpan = TimeSpan.FromMilliseconds(timeout);
            var sw = Stopwatch.StartNew();

            while (sw.Elapsed < timeoutAsTimeSpan)
            {
                using (var session = slave.OpenAsyncSession())
                {
                    try
                    {
                        await session.LoadAsync<User>(id);
                        return;
                    }
                    catch (ConflictException)
                    {
                        await Task.Delay(100);
                    }
                }
            }

            throw new InvalidOperationException($"Waited '{sw.Elapsed}' for conflict to be resolved on '{id}' but it did not happen.");
        }

        private static async Task WaitForConflict(IDocumentStore slave, string id, int timeout = 15_000)
        {
            var timeoutAsTimeSpan = TimeSpan.FromMilliseconds(timeout);
            var sw = Stopwatch.StartNew();

            while (sw.Elapsed < timeoutAsTimeSpan)
            {
                using (var session = slave.OpenAsyncSession())
                {
                    try
                    {
                        await session.LoadAsync<User>(id);
                        await Task.Delay(100);
                    }
                    catch (ConflictException)
                    {
                        return;
                    }
                }
            }

            throw new InvalidOperationException($"Waited '{sw.Elapsed}' for conflict on '{id}' but it did not happen.");
        }

        [Fact]
        public async Task RecordingDeleteAttachmentCommand()
        {
            //Arrange
            var recordFilePath = NewDataPath();

            const string bufferContent = "Menahem";
            var expected = Encoding.ASCII.GetBytes(bufferContent);
            var attachmentStream = new MemoryStream(expected);

            var user = new User { Name = "Gershon" };
            const string id = "users/1";

            const string fileName = "someFileName";

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                await store.Commands().PutAsync(id, null, user, new Dictionary<string, object>
                {
                    {"@collection", "Users"}
                });

                store.Operations.Send(new PutAttachmentOperation(id, fileName, attachmentStream, "application/pdf"));

                store.Operations.Send(new DeleteAttachmentOperation(id, fileName));

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var attachmentResult = store.Operations.Send(new GetAttachmentOperation(id, fileName, AttachmentType.Document, null));

                //Assert
                Assert.Null(attachmentResult);
            }
        }

        [Fact]
        public async Task RecordingPutAttachmentCommand()
        {
            //Arrange
            var recordFilePath = NewDataPath();

            const string bufferContent = "Menahem";
            var expected = Encoding.ASCII.GetBytes(bufferContent);
            var attachmentStream = new MemoryStream(expected);

            var user = new User { Name = "Gershon" };
            const string id = "users/1";

            const string fileName = "someFileName";

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                await store.Commands().PutAsync(id, null, user, new Dictionary<string, object>
                {
                    {"@collection", "Users"}
                });

                store.Operations.Send(new PutAttachmentOperation(id, fileName, attachmentStream, "application/pdf"));

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                var attachmentResult = store.Operations.Send(new GetAttachmentOperation(id, fileName, AttachmentType.Document, null));
                var actual = attachmentResult.Stream.ReadData();

                //Assert
                Assert.Equal(actual, expected);
            }
        }

        [Fact]
        public void StartRecordingWithoutStop_ShouldResultInLegalJson()
        {
            var filePath = NewDataPath();

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
            }

            using (var fileStream = File.OpenRead(filePath))
            using (var zippedStream = new GZipStream(fileStream, CompressionMode.Decompress))
            using (var reader = new StreamReader(zippedStream))
            {
                var fileContent = reader.ReadToEnd();
                JToken.Parse(fileContent);
            }
        }

        [Fact]
        public void ReplayUnsetToZeroStream_ShouldThrowException()
        {
            using (var store = GetDocumentStore())
            {
                using (var replayStream = new MemoryStream(new byte[10]))
                {
                    replayStream.Position = 5;
                    var command = new GetNextOperationIdCommand();
                    store.Commands().Execute(command);

                    Assert.Throws<ArgumentException>(() =>
                        store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result)));
                }
            }
        }

        [Fact]
        public void RecordingDeleteCommand()
        {
            var filePath = NewDataPath();

            var expectedNames = new[] { "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper" };
            var userToDelete = new User { Name = "Avi" };
            var users = expectedNames.Select(n => new User { Name = n }).ToArray();

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                using (var session = store.OpenSession())
                {
                    session.Store(userToDelete);

                    foreach (var user in users)
                    {
                        session.Store(user);
                    }
                    session.SaveChanges();

                    store.Commands().Execute(new DeleteDocumentCommand(userToDelete.Id, null));
                }
                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var actualUserNames = session.Query<User>().Select(u => u.Name).ToArray();

                    //Assert
                    Assert.Equal(actualUserNames, expectedNames);
                }
            }
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public void RecordingPatchWithParametersByQuery(params string[] names)
        {
            var filePath = NewDataPath();

            var users = names.Select(n => new User { Name = n }).ToArray();

            const int newAge = 34;

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                using (var session = store.OpenSession())
                {
                    foreach (var user in users)
                    {
                        session.Store(user);
                    }

                    session.SaveChanges();
                }

                var query = "FROM Users UPDATE {  this.Age = $age; }";
                var parameters = new Parameters { ["age"] = newAge };
                store.Operations
                    .Send(new PatchByQueryOperation(new IndexQuery { Query = query, QueryParameters = parameters }))
                    .WaitForCompletion();

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var replayUsers = session.Query<User>().ToArray();

                    //Assert
                    Assert.All(replayUsers, u => Assert.True(u.Age == newAge, $"The age of {u.Name} should be {newAge} but is {u.Age}"));
                }
            }
        }

        [Fact]
        public void RecordingCountersCommandsAsBatch()
        {
            var recordFilePath = NewDataPath();

            var user = new User { Name = "August" };

            const string id = "users/A-1";
            //Recording
            const string incrementedCounter = "likes";
            const string deletedCounter = "Anger";
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                using (var session = store.OpenSession())
                {
                    session.Store(user, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();

                    session.CountersFor(user).Increment(incrementedCounter, 100);
                    session.CountersFor(user).Increment(deletedCounter, 100);
                    session.SaveChanges();

                    session.CountersFor(user).Delete(deletedCounter);
                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var result = session.CountersFor(user.Id).GetAll();

                    //Assert
                    var actualCounters = result.Select(c => c.Key).ToArray();
                    Assert.Contains(incrementedCounter, actualCounters);
                    Assert.DoesNotContain(deletedCounter, actualCounters);
                }

            }
        }

        [Fact]
        public async Task RecordingRevertDocumentsCommand()
        {
            var filePath = NewDataPath();

            var user = new User { Name = "Baruch" };

            //Recording
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                });

                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                DateTime last;
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();

                    last = DateTime.UtcNow;

                    user.Name = "Bla";
                    session.Store(user);
                    session.SaveChanges();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown))
                {
                    var result = await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                });

                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var revisions = session.Advanced.Revisions.GetFor<User>(user.Id);
                    //Assert
                    Assert.Equal(3, revisions.Count);
                }
            }
        }

        [Fact]
        public void RecordingPatchAsBatch()
        {
            var filePath = NewDataPath();

            const string newName = "Balilo";
            var user = new User { Name = "Baruch" };

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();

                    session.Advanced.Patch(user, u => u.Name, newName);
                    session.SaveChanges();
                }
                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var documents = session.Query<User>().ToArray();

                    //Assert
                    Assert.Equal(documents.First().Name, newName);
                }
            }
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public void RecordingStoreAsBatch(params string[] expected)
        {
            var filePath = NewDataPath();

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                using (var session = store.OpenSession())
                {
                    foreach (var name in expected)
                    {
                        session.Store(new User { Name = name });
                    }

                    session.SaveChanges();
                }
                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var documents = session.Query<User>().ToArray();

                    //Assert
                    Assert.Contains(documents.Select(u => u.Name), n => expected.Contains(n));
                }
            }
        }

        [Fact]
        public void RecordingAddAttachmentAsBatch()
        {
            var filePath = NewDataPath();

            const string bufferContent = "Menahem";
            var expected = Encoding.ASCII.GetBytes(bufferContent);
            var attachmentStream = new MemoryStream(expected);

            const string name = "Avi";
            const string id = "users/A-1";
            const string attachmentName = "someFileName";

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = name }, id);
                    session.Advanced.Attachments.Store(id, attachmentName, attachmentStream);

                    session.SaveChanges();
                }
                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var attachmentResult = session.Advanced.Attachments.Get(id, attachmentName);
                    var actual = attachmentResult.Stream.ReadData();

                    //Assert
                    Assert.Equal(actual, expected);
                }
            }
        }

        [Fact]
        public void RecordingDeleteAttachmentAsBatch()
        {
            var filePath = NewDataPath();

            const string bufferContent = "Menahem";
            var expected = Encoding.ASCII.GetBytes(bufferContent);
            var attachmentStream = new MemoryStream(expected);

            const string name = "Avi";
            const string id = "users/A-1";
            const string attachmentName = "someFileName";

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = name }, id);
                    session.Advanced.Attachments.Store(id, attachmentName, attachmentStream);

                    session.SaveChanges();

                    session.Advanced.Attachments.Delete(id, attachmentName);
                    session.SaveChanges();

                    //                    WaitForUserToContinueTheTest(store);
                }
                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var attachmentResult = session.Advanced.Attachments.Get(id, attachmentName);

                    //Assert
                    Assert.Null(attachmentResult);
                }
            }
        }

        [Fact]
        public void RecordingDeleteAsBatch()
        {
            var filePath = NewDataPath();

            var expectedNames = new[] { "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper" };
            var userToDelete = new User { Name = "Avi" };
            var users = expectedNames.Select(n => new User { Name = n }).ToArray();

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                using (var session = store.OpenSession())
                {
                    session.Store(userToDelete);

                    foreach (var user in users)
                    {
                        session.Store(user);
                    }
                    session.SaveChanges();

                    session.Delete(userToDelete);
                    session.SaveChanges();
                }
                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var actualUserNames = session.Query<User>().Select(u => u.Name).ToArray();

                    //Assert
                    Assert.Equal(actualUserNames, expectedNames);
                }
            }
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public async Task RecordingSmuggler(params string[] expectedNames)
        {
            var recordFilePath = NewDataPath();
            var exportFilePath = NewDataPath();

            var users = expectedNames.Select(n => new User { Name = n }).ToArray();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    foreach (var user in users)
                    {
                        session.Store(user);
                    }

                    session.SaveChanges();
                }

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFilePath);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFilePath);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var actualNames = session.Query<User>().Select(u => u.Name).ToArray();

                    //Assert
                    Assert.All(expectedNames, en =>
                        Assert.True(actualNames.Contains(en),
                            $"{en} should appear one time but found {actualNames.Count(an => an == en)} times"));
                }
            }
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public async Task RecordingPut(params string[] names)
        {
            var recordFilePath = NewDataPath();

            var expectedUsers = names.Select(n => new User { Name = n }).ToArray();

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                foreach (var user in expectedUsers)
                {
                    await store.Commands().PutAsync("user/", null, user, new Dictionary<string, object>
                    {
                        {"@collection", "Users"}
                    });
                }

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var actualUsers = session.Query<User>().ToArray();

                    //Assert
                    Assert.Equal(expectedUsers, actualUsers, new UserComparer());
                }
            }

        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public void RecordingInsertBulk(params string[] names)
        {
            var recordFilePath = NewDataPath();

            var expectedUsers = names.Select(n => new User { Name = n }).ToArray();

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                using (var bulkInsert = store.BulkInsert())
                {
                    foreach (var user in expectedUsers)
                    {
                        bulkInsert.Store(user);
                    }
                }

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var actualUsers = session.Query<User>().ToArray();

                    //Assert
                    Assert.Equal(expectedUsers, actualUsers, new UserComparer());
                }
            }
        }

        private class UserComparer : IEqualityComparer<User>
        {
            public bool Equals(User x, User y)
            {
                return
                    x.Name == y.Name &&
                    x.Age == y.Age;
            }

            public int GetHashCode(User obj)
            {
                throw new NotImplementedException();
            }
        }
    }
}

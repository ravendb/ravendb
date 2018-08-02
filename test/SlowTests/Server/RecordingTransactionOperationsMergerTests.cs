using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server
{
    public class RecordingTransactionOperationsMergerTests : RavenTestBase
    {
        [Fact]
        public void AllDerivedCommandsOfMergedTransactionCommand_MustBeRecordable_ExceptForExceptions()
        {
            var exceptions = new[]
            {
                typeof(TransactionOperationsMerger.MergedTransactionCommand),
                typeof(ExecuteRateLimitedOperations<>),
                typeof(TransactionsRecordingCommand)
            };

            var baseType = typeof(TransactionOperationsMerger.MergedTransactionCommand);
            var types = baseType.Assembly.GetTypes();
            var deriveTypes = types
            .Where(t => baseType.IsAssignableFrom(t)
                        && exceptions.Contains(t) == false);
            Assert.All(deriveTypes, t =>
            {
                //Todo To consider how to check the deseialize possibility
                var i = typeof(TransactionOperationsMerger.IRecordableCommand);
                Assert.True(i.IsAssignableFrom(t), $"{t.Name} should implement {i.Name}");
            });
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

                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
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
            var expected = new User { Name = "Avi"};

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                slave.Maintenance.Server.Send(new ModifyConflictSolverOperation(slave.Database));

                var databaseWatcher = new ExternalReplication(slave.Database, $"ConnectionString-{slave.Identifier}");

                await master.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = databaseWatcher.ConnectionStringName,
                    Database = databaseWatcher.Database,
                    TopologyDiscoveryUrls = master.Urls
                }));

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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
                using (var session = store.OpenSession())
                {
                    var actual = session.Load<User>(id);

                    //Assert
                    Assert.Equal(expected, actual, new UserComparer());
                }
            }
        }

        private static async Task WaitForConflictToBeResolved(IDocumentStore slave, string id)
        {
            while (true)
            {
                using (var session = slave.OpenSession())
                {
                    try
                    {
                        session.Load<User>(id);
                        break;
                    }
                    catch (ConflictException)
                    {
                        await Task.Delay(100);
                    }
                }
            }
        }

        private static async Task WaitForConflict(IDocumentStore slave, string id)
        {
            while (true)
            {
                using (var session = slave.OpenSession())
                {
                    try
                    {
                        session.Load<User>(id);
                        await Task.Delay(100);
                    }
                    catch (ConflictException)
                    {
                        break;
                    }
                }
            }
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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));

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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));

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

            //Todo to change to zip after recording will change to zip
            //            using (var fileStream = File.OpenRead(filePath))
            //            using (var zippedStream = new GZipStream(fileStream, CompressionMode.Decompress))
            //            using (var reader = new StreamReader(zippedStream))
            //            {
            //                var fileContent = File.ReadAllText(filePath);
            //                JToken.Parse(fileContent);
            //            }
            var fileContent = File.ReadAllText(filePath);
            JToken.Parse(fileContent);
        }

        [Fact]
        public void ReplayUnsetToZeroStream_ShouldThrowException()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                using (var store = GetDocumentStore())
                {
                    using (var replayStream = new MemoryStream(new byte[10]))
                    {
                        replayStream.Position = 5;
                        store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
                    }
                }
            });
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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
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

                //Todo To check what that mean "Let's avoid concat of queries, even in test. Use parameters"
                var query = "FROM Users UPDATE {  this.Age = $age; }";
                var parameters = new Parameters { ["age"] = newAge };
                store.Operations
                    .Send(new PatchByQueryOperation(new IndexQuery { Query = query, QueryParameters = parameters }));

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
                using (var session = store.OpenSession())
                {
                    var replayUsers = session.Query<User>().ToArray();

                    //Assert
                    Assert.All(replayUsers, u => Assert.True(u.Age == newAge, $"The age of {u.Name} should be {newAge} but is {u.Age}"));
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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
                //                WaitForUserToContinueTheTest(store);

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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
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

                await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFilePath);
            }

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFilePath);

                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            //Replay
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));

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
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));

                using (var session = store.OpenSession())
                {
                    var actualUsers = session.Query<User>().ToArray();

                    //Assert
                    Assert.Equal(expectedUsers, actualUsers, new UserComparer());
                }
            }
        }

        //        [Fact]
        //        public void RecordingBulkOperation()
        //        {
        //            var recordFilePath = NewDataPath();
        //            const int newAge = 57;
        //            var user = new User { Name = "Avi"};
        //
        //            //Recording
        //            using (var store = GetDocumentStore())
        //            {
        //                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));
        //
        //                using (var session = store.OpenSession())
        //                {
        //                        session.Store(user);
        //                    session.SaveChanges();
        //                }
        //
        //                store.Operations.Send(new PatchOperation(user.Id, null, new PatchRequest
        //                {
        //                    Script = $"this.{nameof(User.Age)} = args.{nameof(User.Age)};",
        //                    Values =
        //                    {
        //                        {nameof(User.Age), newAge }
        //                    }
        //                }));
        //
        //                store.Maintenance.Send(new StopTransactionsRecordingOperation());
        //            }
        //
        //            //Replay
        //            User actualUser;
        //            using (var store = GetDocumentStore())
        //            {
        //                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(recordFilePath));
        //                using (var session = store.OpenSession())
        //                {
        //                    actualUser = session.Load<User>(user.Id);
        //                }
        //            }
        //
        //            //Assert
        //            Assert.True(actualUser.Age == newAge, $"The age of {actualUser.Name} is {actualUser.Age} but should be {newAge}");
        //        }

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

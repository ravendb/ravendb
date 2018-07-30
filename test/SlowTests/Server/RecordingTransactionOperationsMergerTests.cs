using System;
using System.Collections.Generic;
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
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
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

//                WaitForUserToContinueTheTest(store);
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
                return x.Name == y.Name;
            }

            public int GetHashCode(User obj)
            {
                throw new NotImplementedException();
            }
        }

        private class Album
        {
            public string Name { get; set; }
            public string Description { get; set; }
            public string[] Tags { get; set; }
        }
    }
}

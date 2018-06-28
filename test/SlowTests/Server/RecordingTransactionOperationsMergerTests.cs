using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
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
        public void Attachments()
        {
            var dateString = DateTime.Now.ToString("G").Replace(" ", "").Replace(":", "").Replace(@"/", "");
            var filePath = $@"C:\Records\record_{dateString}.json";

            var album = new Album
            {
                Name = "Holidays",
                Description = "Holidays travel pictures of the all family",
                Tags = new[] { "Holidays Travel", "All Family" },
            };

            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));

                using (var session = store.OpenSession())
                using (var picture = File.Open(@"C:\Records\Igal.jpg", FileMode.Open))
                {
                    session.Store(album, "albums/1");

                    session.Advanced.Attachments.Store("albums/1", "Igal.jpg", picture, "image/jpeg");

                    session.SaveChanges();

                }
                store.Maintenance.Send(new StopTransactionsRecordingOperation());
            }

            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
            }
        }

        [Fact]
        public void StartRecordingWithoutStop_ShouldResultInLegalJson()
        {
            var filePath = Path.GetTempFileName();

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
        public void RecordingDeleteCommand()
        {
            var filePath = Path.GetTempFileName();

            var expectedNames = new[] { "Avi" };//, "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper" };
            var users = expectedNames.Select(n => new User { Name = n }).ToArray();
            User[] documents = { };


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

                    store.Commands().Execute(new DeleteDocumentCommand(users.First().Id, null));
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
                    documents = session.Query<User>().ToArray();
                }
            }

            Assert.Empty(documents);
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public void RecordingPatchWithParametersByQuery(params string[] names)
        {
            var filePath = Path.GetTempFileName();


            var users = names.Select(n => new User { Name = n }).ToArray();

            const string AgeKey = "age";
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

                var query = $"FROM Users UPDATE {{  this.Age = ${AgeKey} ; }}";
                Parameters parameters = new Parameters { [AgeKey] = newAge };
                store.Operations
                    .Send(new PatchByQueryOperation(new IndexQuery { Query = query, QueryParameters = parameters }))
                    .WaitForCompletion();

                store.Maintenance.Send(new StopTransactionsRecordingOperation());

            }

            //Replay
            User[] replayUsers;
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(filePath, FileMode.Open))
            {
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
                using (var session = store.OpenSession())
                {
                    replayUsers = session.Query<User>().ToArray();
                }
            }

            //Assert
            Assert.All(replayUsers, u => Assert.True(u.Age == newAge, $"The age of {u.Name} should be {newAge} but is {u.Age}"));
        }

        [Fact]
        public void RecordingPatchAsBatch()
        {
            var filePath = Path.GetTempFileName();

            const string newName = "Balilo";
            var user = new User { Name = "Baruch" };
            User[] documents = { };


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
                    documents = session.Query<User>().ToArray();
                }
                WaitForUserToContinueTheTest(store);
            }

            //Assert
            Assert.Equal(documents.First().Name, newName);
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public void RecordingStoreAsBatch(params string[] expected)
        {
            var filePath = Path.GetTempFileName();
            User[] documents = { };

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
                    documents = session.Query<User>().ToArray();
                }
            }

            //Assert
            Assert.Contains(documents.Select(u => u.Name), n => expected.Contains(n));
        }

        [Fact]
        public void RecordingDeleteAsBatch()
        {
            var filePath = System.IO.Path.GetTempFileName();

            var toRemove = new User { Name = "ravenDb" };
            User[] documents = { };

            //Recording
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StartTransactionsRecordingOperation(filePath));
                using (var session = store.OpenSession())
                {
                    session.Store(toRemove);
                    session.SaveChanges();

                    session.Delete(toRemove);
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
                    documents = session.Query<User>().ToArray();
                }
            }

            //Assert
            Assert.Empty(documents);
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public async Task RecordingSmuggler(params string[] names)
        {
            var recordFilePath = Path.GetTempFileName();
            var exportFilePath = Path.GetTempFileName();

            var expectedUsers = names.Select(n => new User { Name = n }).ToArray();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    foreach (var user in expectedUsers)
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
            User[] actualUsers;
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));
                using (var session = store.OpenSession())
                {
                    actualUsers = session.Query<User>().ToArray();
                }
            }

            //Assert
            var errors = expectedUsers
                .Where(eu => actualUsers.Count(au => au.Name == eu.Name) != 1)
                .Select(eu => $"{eu.Name} should appear one time but found {actualUsers.Count(au => au.Name == eu.Name)} times")
                .ToArray();

            Assert.True(!errors.Any(), string.Join(Environment.NewLine, errors));
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public async Task RecordingPut(params string[] names)
        {
            var recordFilePath = Path.GetTempFileName();

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
            User[] actualUsers;
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));

                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    actualUsers = session.Query<User>().ToArray();
                }
            }

            //Assert
            Assert.Equal(expectedUsers, actualUsers, new UserComparer());
        }

        [Theory]
        [InlineData("Avi")]
        [InlineData("Avi", "Baruch", "Charlotte", "Dylan", "Eli", "Fabia", "George", "Harper")]
        public void RecordingInsertBulk(params string[] names)
        {
            var recordFilePath = Path.GetTempFileName();

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
            User[] actualUsers;
            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream));

                using (var session = store.OpenSession())
                {
                    actualUsers = session.Query<User>().ToArray();
                }
            }

            //Assert
            Assert.Equal(expectedUsers, actualUsers, new UserComparer());
        }

        //        [Fact]
        //        public void RecordingBulkOperation()
        //        {
        //            var recordFilePath = Path.GetTempFileName();
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
        //                WaitForUserToContinueTheTest(store);
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

using System;
using System.IO;
using System.Threading.Tasks;
using FastTests.Sharding;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Smuggler
{
    public class ExportAndImport : ShardedTestBase
    {
        public ExportAndImport(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanExportAndImport()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetShardedDocumentStore())
                {
                    var user1 = new User { Name = "Name1", LastName = "LastName1", Age = 5 };
                    using (var session = store1.OpenAsyncSession())
                    {
                        
                        await session.StoreAsync(user1, "users/1");
                        await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                        await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                        await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(DateTime.Now, 59d, "watches/fitbit");
                        session.TimeSeriesFor("users/3", "Heartrate")
                            .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");

                        session.CountersFor("users/2").Increment("Downloads", 100);

                        await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                        await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                        await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        {
                            session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                            session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                            session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                            await session.SaveChangesAsync();
                        }
                        
                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    //await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)); //TODO - EFRAT
                    await Task.Delay(TimeSpan.FromSeconds(20));
                    using (var store2 = GetDocumentStore(new Options {ModifyDatabaseName = s => $"{s}_2"}))
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        WaitForUserToContinueTheTest(store2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                        Assert.Equal(4, stats.CountOfDocuments);

                        using (var session = store2.OpenSession())
                        {
                            var val = session.TimeSeriesFor("users/1", "Heartrate")
                                .Get(DateTime.MinValue, DateTime.MaxValue);

                            Assert.Equal(1, val.Length);

                            val = session.TimeSeriesFor("users/3", "Heartrate")
                                .Get(DateTime.MinValue, DateTime.MaxValue);

                            Assert.Equal(1, val.Length);

                            var counterValue = session.CountersFor("users/2").Get("Downloads");
                            Assert.Equal(100, counterValue.Value);
                        }

                        using (var session = store2.OpenAsyncSession())
                        {
                            for (var i = 0; i < names.Length; i++)
                            {
                                var user = await session.LoadAsync<User>("users/" + (i + 1));
                                var metadata = session.Advanced.GetMetadataFor(user);
                                var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                                Assert.Equal(1, attachments.Length);
                                var attachment = attachments[0];
                                Assert.Equal(names[i], attachment.GetString(nameof(AttachmentName.Name)));
                                var hash = attachment.GetString(nameof(AttachmentName.Hash));
                                if (i == 0)
                                {
                                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                                    Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                                else if (i == 1)
                                {
                                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                                    Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                                else if (i == 2)
                                {
                                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                                    Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                            }
                        }
                        WaitForUserToContinueTheTest(store2);
                    }

                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImportEncrypted()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetShardedDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {

                        await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                        await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                        await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(DateTime.Now, 59d, "watches/fitbit");
                        session.TimeSeriesFor("users/3", "Heartrate")
                            .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");

                        session.CountersFor("users/2").Increment("Downloads", 100);

                        await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                        await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                        await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        {
                            session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                            session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                            session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                            await session.SaveChangesAsync();
                        }

                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }, file);
                    //await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                    await Task.Delay(TimeSpan.FromSeconds(20));
                    using (var store2 = GetDocumentStore(new Options {ModifyDatabaseName = s => $"{s}_2"}))
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions {EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="},
                            file);
                        //WaitForUserToContinueTheTest(store2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                        Assert.Equal(4, stats.CountOfDocuments);

                        using (var session = store2.OpenSession())
                        {
                            var val = session.TimeSeriesFor("users/1", "Heartrate")
                                .Get(DateTime.MinValue, DateTime.MaxValue);

                            Assert.Equal(1, val.Length);

                            val = session.TimeSeriesFor("users/3", "Heartrate")
                                .Get(DateTime.MinValue, DateTime.MaxValue);

                            Assert.Equal(1, val.Length);

                            var counterValue = session.CountersFor("users/2").Get("Downloads");
                            Assert.Equal(100, counterValue.Value);
                        }

                        using (var session = store2.OpenAsyncSession())
                        {
                            for (var i = 0; i < names.Length; i++)
                            {
                                var user = await session.LoadAsync<User>("users/" + (i + 1));
                                var metadata = session.Advanced.GetMetadataFor(user);
                                var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                                Assert.Equal(1, attachments.Length);
                                var attachment = attachments[0];
                                Assert.Equal(names[i], attachment.GetString(nameof(AttachmentName.Name)));
                                var hash = attachment.GetString(nameof(AttachmentName.Hash));
                                if (i == 0)
                                {
                                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                                    Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                                else if (i == 1)
                                {
                                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                                    Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                                else if (i == 2)
                                {
                                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                                    Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                            }
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImport2()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                {
                    var user1 = new User { Name = "Name1", LastName = "LastName1", Age = 5 };
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                    using (var session = store1.OpenAsyncSession())
                    {

                        await session.StoreAsync(user1, "users/1");
                        await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                        await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                        await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(DateTime.Now, 59d, "watches/fitbit");
                        session.TimeSeriesFor("users/3", "Heartrate")
                            .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");
                        
                        session.CountersFor("users/2").Increment("Downloads", 100);
                        
                        await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                        await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                        await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        {
                            session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                            session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                            session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                            await session.SaveChangesAsync();
                        }

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>("users/2");
                        user.Age = 46;

                        session.Delete("users/4");
                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)); //TODO - EFRAT
                    
                    using (var store2 = GetShardedDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await Task.Delay(TimeSpan.FromSeconds(20));

                        WaitForUserToContinueTheTest(store1);
                        //await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        using (var session = store2.OpenSession())
                        {
                            // var val = session.TimeSeriesFor("users/1", "Heartrate")
                            //     .Get(DateTime.MinValue, DateTime.MaxValue);
                            //
                            // Assert.Equal(1, val.Length);
                            //
                            // val = session.TimeSeriesFor("users/3", "Heartrate")
                            //     .Get(DateTime.MinValue, DateTime.MaxValue);
                            //
                            // Assert.Equal(1, val.Length);

                            // var counterValue = session.CountersFor("users/2").Get("Downloads");
                            // Assert.Equal(100, counterValue.Value);
                        }

                        using (var session = store2.OpenAsyncSession())
                        {
                            User user;
                            for (var i = 0; i < names.Length; i++)
                            {
                                user = await session.LoadAsync<User>("users/" + (i + 1));
                                var metadata = session.Advanced.GetMetadataFor(user);
                                var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                                Assert.Equal(1, attachments.Length);
                                var attachment = attachments[0];
                                Assert.Equal(names[i], attachment.GetString(nameof(AttachmentName.Name)));
                                var hash = attachment.GetString(nameof(AttachmentName.Hash));
                                if (i == 0)
                                { 
                                    Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.TimeSeries));
                                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                                    Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                                else if (i == 1)
                                {
                                    Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Counters));
                                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                                    Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                                else if (i == 2)
                                {
                                    Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.TimeSeries));
                                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                                    Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                                }
                            }

                            user = await session.LoadAsync<User>("users/4");
                            Assert.Null(user);
                        }
                    }

                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}

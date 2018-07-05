using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10666 : RavenTestBase
    {
        [Fact]
        public void DocumentSessionAttachments_ShouldRespectSessionDatabase()
        {
            using (var store = GetDocumentStore())
            {
                var dbName = store.Database + "_myCustomDB";
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName)));

                try
                {
                    using (var session = store.OpenSession(dbName))
                    {
                        session.Store(new User
                        {
                            Name = "Jerry"
                        }, "users/1-A");

                        session.SaveChanges();
                    }

                    const string fileName = "001.txt";
                    using (var writer = new StreamWriter(File.Open(fileName, FileMode.Create)))
                    {
                        writer.Write("1010011010");
                    }

                    using (var session = store.OpenSession(dbName))
                    using (var file = File.Open(fileName, FileMode.Open))
                    {
                        session.Advanced.Attachments.Store("users/1-A", fileName, file, "text");
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession(dbName))
                    {
                        Assert.True(session.Advanced.Attachments.Exists("users/1-A", fileName));

                        using (var file1 = session.Advanced.Attachments.Get("users/1-A", fileName))
                        {
                            using (var reader = new StreamReader(file1.Stream))
                            {
                                var str = reader.ReadLine();
                                Assert.Equal("1010011010", str);
                            }

                            var attachmentDetails = file1.Details;

                            Assert.Equal(fileName, attachmentDetails.Name);
                            Assert.Equal("text", attachmentDetails.ContentType);
                            Assert.Equal("users/1-A", attachmentDetails.DocumentId);
                            Assert.NotNull(attachmentDetails.Hash);
                            Assert.NotNull(attachmentDetails.Size);
                            Assert.NotNull(attachmentDetails.ChangeVector);
                        }
                    }
                }
                finally
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, hardDelete: true));
                }
            }
        }

        [Fact]
        public async Task DocumentSessionAttachments_ShouldRespectSessionDatabase_Async()
        {
            using (var store = GetDocumentStore())
            {
                var dbName = store.Database + "_myCustomDB";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(dbName)));

                try
                {
                    using (var session = store.OpenAsyncSession(dbName))
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "Jerry"
                        }, "users/1-A");

                        await session.SaveChangesAsync();
                    }

                    const string fileName = "001.txt";
                    using (var writer = new StreamWriter(File.Open(fileName, FileMode.Create)))
                    {
                        writer.Write("1010011010");
                    }

                    using (var session = store.OpenAsyncSession(dbName))
                    using (var file = File.Open(fileName, FileMode.Open))
                    {
                        session.Advanced.Attachments.Store("users/1-A", fileName, file, "text");
                        await session.SaveChangesAsync();
                    }

                    using (var session = store.OpenAsyncSession(dbName))
                    {
                        Assert.True(await session.Advanced.Attachments.ExistsAsync("users/1-A", fileName));

                        using (var file = await session.Advanced.Attachments.GetAsync("users/1-A", fileName))
                        {
                            using (var reader = new StreamReader(file.Stream))
                            {
                                var str = reader.ReadLine();
                                Assert.Equal("1010011010", str);
                            }

                            var attachmentDetails = file.Details;

                            Assert.Equal(fileName, attachmentDetails.Name);
                            Assert.Equal("text", attachmentDetails.ContentType);
                            Assert.Equal("users/1-A", attachmentDetails.DocumentId);
                            Assert.NotNull(attachmentDetails.Hash);
                            Assert.NotNull(attachmentDetails.Size);
                            Assert.NotNull(attachmentDetails.ChangeVector);
                        }
                    }
                }
                finally
                {
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, hardDelete: true));
                }
            }
        }
    }
}

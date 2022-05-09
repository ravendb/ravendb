using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17082 : ReplicationTestBase
    {
        public RavenDB_17082(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RevertRevisionWithMoreInfo()
        {
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            DateTime last = default;
            using (var store = GetDocumentStore())
            {

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1"}, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                    {
                        session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                        await session.SaveChangesAsync();
                    }
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Delete("Downloads");
                    session.Advanced.Attachments.Delete("users/1", "ImGgE/jPeG");
                   await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(5, rev.Count);

                    Assert.Equal("Name1", rev[0].Name);

                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.HasAttachments.ToString(), flags);
                    Assert.Contains(DocumentFlags.HasCounters.ToString(), flags);
                    Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Counters));
                    Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.RevisionCounters));
                }
            }
        }

        [Fact]
        public async Task RemoveResolveFlagAfterRevert()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);

                DateTime last = default;

                using (var session = store1.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();


                    await session.StoreAsync(new Person(), "marker");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store2, store1);
                WaitForDocument(store1, "marker");

                last = DateTime.UtcNow;
                using (var session = store1.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name3"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();
                }
                var db = await Databases.GetDocumentDatabaseInstanceFor(store1);
                WaitForUserToContinueTheTest(store1);
                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                Assert.Equal(4, result.ScannedRevisions);
                Assert.Equal(1, result.ScannedDocuments);
                Assert.Equal(1, result.RevertedDocuments);

                using (var session = store1.OpenAsyncSession())
                {
                    var foo = await session.LoadAsync<User>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(foo);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.Reverted.ToString(), flags);
                    Assert.False(flags.Contains(DocumentFlags.Resolved.ToString()));

                    var persons = await session.Advanced.Revisions.GetForAsync<Person>("foo/bar");
                    Assert.Equal(5, persons.Count);

                    Assert.Equal("Name2", persons[0].Name);
                    metadata = session.Advanced.GetMetadataFor(persons[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Reverted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name3", persons[1].Name);
                    metadata = session.Advanced.GetMetadataFor(persons[1]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision ).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name2", persons[2].Name);
                    metadata = session.Advanced.GetMetadataFor(persons[2]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Resolved).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task RevertRevisionWithAttachments()
        {
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            DateTime last = default;
            using (var store = GetDocumentStore())
            {

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" }, "users/1");
                    await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                    {
                        session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                        await session.SaveChangesAsync();
                    }
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                   await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                   {
                        session.Advanced.Attachments.Store("users/1", names[1], backgroundStream, "ImGgE/jPeG");
                        await session.SaveChangesAsync();
                   }
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(4, rev.Count);
                    var cv = session.Advanced.GetChangeVectorFor(rev[0]);
                    Assert.Equal("Name1", rev[0].Name);
                    Assert.NotNull(await session.Advanced.Attachments.GetRevisionAsync("users/1", names[0], cv));
                    Assert.Null(await session.Advanced.Attachments.GetRevisionAsync("users/1", names[1], cv));

                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.HasAttachments.ToString(), flags);
                    var att = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, att.Length);
                    Assert.Equal(names[0], att[0].Name);
                }
            }
        }

        [Fact]
        public async Task RevertRevisionWithDeleteAttachments()
        {
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            DateTime last = default;
            using (var store = GetDocumentStore())
            {

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" }, "users/1");
                    await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                    {
                        session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                        await session.SaveChangesAsync();
                    }
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Attachments.Delete("users/1", names[0]);
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(4, rev.Count);
                    var cv = session.Advanced.GetChangeVectorFor(rev[0]);
                    Assert.Equal("Name1", rev[0].Name);
                    Assert.NotNull(await session.Advanced.Attachments.GetRevisionAsync("users/1", names[0], cv));

                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.HasAttachments.ToString(), flags);
                    var att = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, att.Length);
                    Assert.Equal(names[0], att[0].Name);
                }
            }
        }

        [Fact]
        public async Task RevertRevisionWithCounters()
        {
            DateTime last = default;
            using (var store = GetDocumentStore())
            {

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" }, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Downloads2", 200);
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(4, rev.Count);

                    Assert.Equal("Name1", rev[0].Name);
                    var counters = await session.CountersFor(rev[0]).GetAllAsync();
                    Assert.Equal(1, counters.Count);
                    Assert.Equal(100 , counters["Downloads"]);


                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Counters));
                    Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.RevisionCounters));
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.HasCounters.ToString(), flags);
                    var counter = await session.CountersFor(user).GetAllAsync();
                    Assert.Equal(1, counter.Count);
                    Assert.Equal("Downloads", counter.First().Key);
                }
            }
        }

        [Fact]
        public async Task RevertRevisionWithDeleteCounters()
        {
            DateTime last = default;
            using (var store = GetDocumentStore())
            {

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" }, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Delete("Downloads");
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(4, rev.Count);

                    Assert.Equal("Name1", rev[0].Name);
                    var counters = await session.CountersFor(rev[0]).GetAllAsync();
                    Assert.Equal(1, counters.Count);
                    Assert.Equal(100, counters["Downloads"]);


                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Counters));
                    Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.RevisionCounters));
                    Assert.Contains(DocumentFlags.HasCounters.ToString(), flags);
                    var counter = await session.CountersFor(user).GetAllAsync();
                    Assert.Equal(1, counter.Count);
                    Assert.Equal("Downloads", counter.First().Key);
                }
            }
        }

        [Fact]
        public async Task RemoveRevertFlagAfterNewInfo1()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                DateTime last = default;

                using (var session = store.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();

                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                Assert.Equal(2, result.ScannedRevisions);
                Assert.Equal(1, result.ScannedDocuments);
                Assert.Equal(1, result.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var persons = await session.Advanced.Revisions.GetForAsync<Person>("foo/bar");
                    Assert.Equal(3, persons.Count);

                    Assert.Equal("Name1", persons[0].Name);
                    var metadata = session.Advanced.GetMetadataFor(persons[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Reverted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                    {
                        session.Advanced.Attachments.Store("foo/bar", "background-photo.jpg", backgroundStream, "ImGgE/jPeG");
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store.OpenAsyncSession())
                {
                    var foo = await session.LoadAsync<Person>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(foo);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.False(flags.Contains(DocumentFlags.Reverted.ToString()));

                    var persons = await session.Advanced.Revisions.GetForAsync<Person>("foo/bar");
                    Assert.Equal(4, persons.Count);

                    Assert.Equal("Name1", persons[0].Name);
                    metadata = session.Advanced.GetMetadataFor(persons[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.HasAttachments).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task RemoveRevertFlagAfterNewInfo2()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                DateTime last = default;

                using (var session = store.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();

                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                Assert.Equal(2, result.ScannedRevisions);
                Assert.Equal(1, result.ScannedDocuments);
                Assert.Equal(1, result.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var persons = await session.Advanced.Revisions.GetForAsync<Person>("foo/bar");
                    Assert.Equal(3, persons.Count);


                    Assert.Equal("Name1", persons[0].Name);
                    var metadata = session.Advanced.GetMetadataFor(persons[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Reverted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                    session.CountersFor("foo/bar").Increment("Downloads", 100);
                    await session.SaveChangesAsync();
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenAsyncSession())
                {
                    var foo = await session.LoadAsync<Person>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(foo);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.False(flags.Contains(DocumentFlags.Reverted.ToString()));

                    var persons = await session.Advanced.Revisions.GetForAsync<Person>("foo/bar");
                    Assert.Equal(4, persons.Count);

                    Assert.Equal("Name1", persons[0].Name);
                    metadata = session.Advanced.GetMetadataFor(persons[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.HasCounters).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task RevertToTombstone()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                DateTime last = default;

                using (var session = store.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    session.CountersFor("foo/bar").Increment("Downloads", 100);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();

                }
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                Assert.Equal(4, result.ScannedRevisions);
                Assert.Equal(1, result.ScannedDocuments);
                Assert.Equal(1, result.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var foo = await session.LoadAsync<User>("foo/bar");
                    Assert.Null(foo);
                    var persons = await session.Advanced.Revisions.GetForAsync<Person>("foo/bar");
                    Assert.Equal(5, persons.Count);

                    var metadata = session.Advanced.GetMetadataFor(persons[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.DeleteRevision | DocumentFlags.Reverted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var person = new Person
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(person, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var foo = await session.LoadAsync<User>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(foo);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.DoesNotContain(DocumentFlags.HasCounters.ToString(), flags);
                }
            }
        }

        [Fact]
        public async Task RevertRevisionWithTimeSeries()
        {
            DateTime last = default;
            using (var store = GetDocumentStore())
            {

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" }, "users/1");
                    var ts = session.TimeSeriesFor("users/1", "Toli");
                    ts.Append(DateTime.Today.AddDays(-1), 10);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name2", LastName = "LastName1" }, "users/1");
                    var ts = session.TimeSeriesFor("users/1", "Mitzi");
                    ts.Append(DateTime.Today, 30);
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(5, rev.Count);

                    Assert.Equal("Name1", rev[0].Name);
                    var ts = await session.TimeSeriesFor(rev[0], "Toli").GetAsync();
                    Assert.Equal(1, ts.Length);
                    Assert.Equal(10, ts[0].Value);

                    ts = await session.TimeSeriesFor(rev[0], "Mitzi").GetAsync();
                    Assert.Equal(1, ts.Length);
                    Assert.Equal(30, ts[0].Value);

                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("Name1", user.Name);
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.TimeSeries));
                    Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.RevisionTimeSeries));
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.HasTimeSeries.ToString(), flags);
                    ts = await session.TimeSeriesFor(user, "Toli").GetAsync();
                    Assert.Equal(1, ts.Length);
                    Assert.Equal(10, ts[0].Value);
                    ts = await session.TimeSeriesFor(user, "Mitzi").GetAsync();
                    Assert.Equal(1, ts.Length);
                    Assert.Equal(30, ts[0].Value);
                }
            }
        }
    }
}

using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Json;
using Raven.Server.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20883 : RavenTestBase
{
    public RavenDB_20883(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Attachments)]
    public async Task Number_Of_Attachments_Needs_To_Match_Async()
    {
        using (var store = GetDocumentStore())
        {
            store.OnBeforeStore += (_, args) =>
            {
                if (args.Entity is User)
                {
                    args.DocumentMetadata["@collection"] = "Users";
                    args.DocumentMetadata["Prop"] = Guid.NewGuid().ToString();
                }
            };

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "Test" };

                await session.StoreAsync(user, "users/1");

                session.Advanced.Attachments.Store(user, "attachment-1", new MemoryStream(new byte[] { 1, 2, 3 }));

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");

                var metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Attachments));
                Assert.Contains(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);

                var attachments = session.Advanced.Attachments.GetNames(user);
                Assert.NotEmpty(attachments);
                Assert.Equal(1, attachments.Length);

                user.Name = "Test 2";
                await session.SaveChangesAsync();

                metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Attachments));
                Assert.Contains(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);

                attachments = session.Advanced.Attachments.GetNames(user);
                Assert.NotEmpty(attachments);
                Assert.Equal(1, attachments.Length);

                session.Advanced.Clear();
                user = await session.LoadAsync<User>("users/1");

                metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Attachments));
                Assert.Contains(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);

                attachments = session.Advanced.Attachments.GetNames(user);
                Assert.NotEmpty(attachments);
                Assert.Equal(1, attachments.Length);


            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
    public async Task Number_Of_TimeSeries_Needs_To_Match_Async()
    {
        using (var store = GetDocumentStore())
        {
            store.OnBeforeStore += (_, args) =>
            {
                if (args.Entity is User)
                {
                    args.DocumentMetadata["@collection"] = "Users";
                    args.DocumentMetadata["Prop"] = Guid.NewGuid().ToString();
                }
            };

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "Test" };

                await session.StoreAsync(user, "users/1");

                session.TimeSeriesFor(user, "timeseries-1").Append(DateTime.Now, 1);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");

                var metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.TimeSeries));
                Assert.Contains(DocumentFlags.HasTimeSeries.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);

                user.Name = "Test 2";
                await session.SaveChangesAsync();

                metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.TimeSeries));
                Assert.Contains(DocumentFlags.HasTimeSeries.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);

                session.Advanced.Clear();
                user = await session.LoadAsync<User>("users/1");

                metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.TimeSeries));
                Assert.Contains(DocumentFlags.HasTimeSeries.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Counters)]
    public async Task Number_Of_Counters_Needs_To_Match_Async()
    {
        using (var store = GetDocumentStore())
        {
            store.OnBeforeStore += (_, args) =>
            {
                if (args.Entity is User)
                {
                    args.DocumentMetadata["@collection"] = "Users";
                    args.DocumentMetadata["Prop"] = Guid.NewGuid().ToString();
                }
            };

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "Test" };

                await session.StoreAsync(user, "users/1");

                session.CountersFor(user).Increment("counter-1", 3);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");

                var metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Counters));
                Assert.Contains(DocumentFlags.HasCounters.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);

                user.Name = "Test 2";
                await session.SaveChangesAsync();

                metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Counters));
                Assert.Contains(DocumentFlags.HasCounters.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);

                session.Advanced.Clear();
                user = await session.LoadAsync<User>("users/1");

                metadata = (MetadataAsDictionary)session.Advanced.GetMetadataFor(user);
                foreach (var _ in metadata)
                {
                }
                Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Counters));
                Assert.Contains(DocumentFlags.HasCounters.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                Assert.False(metadata.Changed);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.ClusterTransactions)]
    public async Task Changing_Metadata_Should_Not_Remove_Any_System_Properties()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "Test" };

                await session.StoreAsync(user, "users/1");

                session.CountersFor(user).Increment("counter-1", 3);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user = await session.LoadAsync<User>("users/1");
                user.Name = "Test2";

                session.Advanced.GetMetadataFor(user)["Prop"] = Guid.NewGuid().ToString();

                var e = await Assert.ThrowsAsync<RavenException>(() => session.SaveChangesAsync());
                Assert.Contains("has counters, this is not supported in cluster wide transaction", e.Message);
            }
        }
    }

    private class User
    {
        public string Id { get; set; }

        public string Name { get; set; }
    }
}

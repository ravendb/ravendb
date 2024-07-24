using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.ETL;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19696 : RavenTestBase
{
    public RavenDB_19696(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task EtlCanSendDocWithTimeSeriesAndAttachment(Options options)
    {
        const string id = "users/1";
        using (var store = GetDocumentStore(options))
        using (var replica = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "ayende"
                }, id);

                session.TimeSeriesFor(id, "Heartrate").Append(DateTime.Now, 1);

                await using var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 });
                session.Advanced.Attachments.Store(id, "profileImage", backgroundStream, "ImGgE/jPeG");

                await session.SaveChangesAsync();
            }
             
            var taskName = "etl-test";
            var csName = "cs-test";

            var configuration = new RavenEtlConfiguration
            {
                ConnectionStringName = csName, 
                Name = taskName, 
                Transforms =
                {
                    new Transformation
                    {
                        Name = "S1", 
                        Collections = { "Users" }
                    }
                }
            };

            var connectionString = new RavenConnectionString
            {
                Name = csName, 
                TopologyDiscoveryUrls = replica.Urls, 
                Database = replica.Database,
            };

            var etlDone = Etl.WaitForEtlToComplete(store);
            
            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = replica.OpenSession())
            {
                var user = session.Load<User>("users/1");
                Assert.NotNull(user);
                Assert.Equal("ayende", user.Name);

                var metadata = session.Advanced.GetMetadataFor(user);
                Assert.Equal("Users", metadata[Constants.Documents.Metadata.Collection]);
                Assert.NotEmpty(session.TimeSeriesFor(user, "Heartrate").Get());
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task EtlCanSendDocWithTimeSeriesCountersAndAttachments(Options options)
    {
        const string id = "users/1";
        using (var store = GetDocumentStore(options))
        using (var replica = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "ayende"
                }, id);

                session.TimeSeriesFor(id, "Heartrate").Append(DateTime.Now, 1);
                session.CountersFor(id).Increment("Followers", 1000000);

                await using var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 });
                session.Advanced.Attachments.Store(id, "profileImage", backgroundStream, "ImGgE/jPeG");

                await session.SaveChangesAsync();
            }
             
            var taskName = "etl-test";
            var csName = "cs-test";

            var configuration = new RavenEtlConfiguration
            {
                ConnectionStringName = csName, 
                Name = taskName, 
                Transforms =
                {
                    new Transformation
                    {
                        Name = "S1", 
                        Collections = { "Users" }
                    }
                }
            };

            var connectionString = new RavenConnectionString
            {
                Name = csName, 
                TopologyDiscoveryUrls = replica.Urls, 
                Database = replica.Database,
            };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = replica.OpenSession())
            {
                var user = session.Load<User>("users/1");
                Assert.NotNull(user);
                Assert.Equal("ayende", user.Name);

                var metadata = session.Advanced.GetMetadataFor(user);
                Assert.Equal("Users", metadata[Constants.Documents.Metadata.Collection]);
                Assert.Equal(1000000, session.CountersFor(user).Get("Followers"));
                Assert.NotEmpty(session.TimeSeriesFor(user, "Heartrate").Get());


               var attachment =  session.Advanced.Attachments.Get(id, "profileImage");

            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task EtlCanSendDocWithCountersAndAttachments(Options options)
    {
        const string id = "users/1";
        using (var store = GetDocumentStore(options))
        using (var replica = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "ayende"
                }, id);

                session.CountersFor(id).Increment("Followers", 1000000);

                await using var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 });
                session.Advanced.Attachments.Store(id, "profileImage", backgroundStream, "ImGgE/jPeG");

                await session.SaveChangesAsync();
            }
             
            var taskName = "etl-test";
            var csName = "cs-test";

            var configuration = new RavenEtlConfiguration
            {
                ConnectionStringName = csName, 
                Name = taskName, 
                Transforms =
                {
                    new Transformation
                    {
                        Name = "S1", 
                        Collections = { "Users" }
                    }
                }
            };

            var connectionString = new RavenConnectionString
            {
                Name = csName, 
                TopologyDiscoveryUrls = replica.Urls, 
                Database = replica.Database,
            };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = replica.OpenSession())
            {
                var user = session.Load<User>("users/1");
                Assert.NotNull(user);
                Assert.Equal("ayende", user.Name);

                var metadata = session.Advanced.GetMetadataFor(user);
                Assert.Equal("Users", metadata[Constants.Documents.Metadata.Collection]);
                Assert.Equal(1000000, session.CountersFor(user).Get("Followers"));
            }
        }
    }
}

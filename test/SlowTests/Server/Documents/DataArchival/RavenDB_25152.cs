using System;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
using Tests.Infrastructure;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;

namespace SlowTests.Server.Documents.DataArchival;

public class RavenDB_25152 : ReplicationTestBase
{
    public RavenDB_25152(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Replication)]
    public async Task PropagateDocumentUnarchivalInReplication()
    {
        using (var src = GetDocumentStore())
        using (var dst = GetDocumentStore())
        {
            var now = SystemTime.UtcNow;
            
            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name" };
            var archiveAt = now.AddMinutes(5);
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(company, "companies/1-A");
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = archiveAt.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }
            
            var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };

            await DataArchivalHelper.SetupDataArchival(src, Server.ServerStore, config);

            var database = await Databases.GetDocumentDatabaseInstanceFor(src);
            database.Time.UtcDateTime = () => now.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await SetupReplicationAsync(src, dst);
            
            var archivedCompany = await WaitForDocumentToReplicateAsync<User>(dst, "companies/1-A", TimeSpan.FromSeconds(15));
            Assert.NotNull(archivedCompany);
            
            using (var session = dst.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Company>("companies/1-A");
                var metadata = session.Advanced.GetMetadataFor(doc);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);
            }
            
            // Unarchive document using patch
            var operation = await src.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery()
            {
                Query = "from Companies update { archived.unarchive(this) }"
            }));
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            using (var s1 = src.OpenSession())
            {
                s1.Store(new User(), "marker/doc");
                s1.SaveChanges();
            }
            
            var marker = await WaitForDocumentToReplicateAsync<User>(dst, "marker/doc", TimeSpan.FromSeconds(15));
            Assert.NotNull(marker);
            
            using (var session = dst.OpenAsyncSession())
            {
                // Make sure that item is unarchived so markers are gone 
                var unarchivedCompany = await session.LoadAsync<Company>("companies/1-A");
                var metadata = session.Advanced.GetMetadataFor(unarchivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.DoesNotContain(Constants.Documents.Metadata.Archived, metadata.Keys);
            }
        }
    }
}

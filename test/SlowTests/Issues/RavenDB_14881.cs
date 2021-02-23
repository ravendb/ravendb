using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14881 : RavenTestBase
    {
        public RavenDB_14881(ITestOutputHelper output)
            : base(output)
        {
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task can_get_detailed_collection_statistics(bool compressed)
        {
            string strCollectionName = "Companies";
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (!compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration
                        {
                            Collections = null,
                            CompressRevisions = false
                        };
                    }
                }
            }))
            {
                // configure revisions for the collection
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        {
                            "Companies", new RevisionsCollectionConfiguration()
                            {
                                Disabled = false
                            }
                        }
                    }
                };

                var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                

                // insert sample data
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 20; i++)
                    {
                        bulk.Store(new Company
                        {
                            Id = "company/" + i,
                            Name = Convert.ToBase64String(Sodium.GenerateRandomBuffer(128 * 8))
                        });
                    }
                }

                // get detailed collection statistics before we are going to change some data
                // right now there shouldn't be any revisions
                var detailedCollectionStats_beforeDataChanged = await store.Maintenance.SendAsync(new GetDetailedCollectionStatisticsOperation());
                Assert.Equal(strCollectionName, detailedCollectionStats_beforeDataChanged.Collections[strCollectionName].Name);
                long sizeInBytesWithoutRevisions = detailedCollectionStats_beforeDataChanged.Collections[strCollectionName].Size.SizeInBytes;
                Assert.True(sizeInBytesWithoutRevisions > 0);

                // change some data
                for (int i = 0; i < 200; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>("company/1");
                        company.Name = Convert.ToBase64String(Sodium.GenerateRandomBuffer(128*8));
                        await session.StoreAsync(company);
                        await session.SaveChangesAsync();
                    }
                }

                // get the revisions for the changed document
                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<Company>("company/1", 0, 200);
                    Assert.Equal(200, revisions.Count);
                }

                // query the detailed collection statistics again, to check if the physical size changed after the revisions were created
                var detailedCollectionStats_afterDataChanged = await store.Maintenance.SendAsync(new GetDetailedCollectionStatisticsOperation());
                Assert.Equal(20, detailedCollectionStats_afterDataChanged.Collections[strCollectionName].CountOfDocuments);
                long sizeInBytesWithRevisions = detailedCollectionStats_afterDataChanged.Collections[strCollectionName].Size.SizeInBytes;
                Assert.True(sizeInBytesWithRevisions > sizeInBytesWithoutRevisions);

                var collectionDetails = detailedCollectionStats_afterDataChanged.Collections[strCollectionName];
                Assert.True(collectionDetails.DocumentsSize.SizeInBytes > 0);
                Assert.True(collectionDetails.RevisionsSize.SizeInBytes > 0);
                Assert.Equal(collectionDetails.Size.SizeInBytes, collectionDetails.DocumentsSize.SizeInBytes + collectionDetails.RevisionsSize.SizeInBytes + collectionDetails.TombstonesSize.SizeInBytes);
            }
        }
    }
}

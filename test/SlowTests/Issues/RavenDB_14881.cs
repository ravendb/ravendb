using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14881 : RavenTestBase
    {
        public RavenDB_14881(ITestOutputHelper output)
            :base(output)
        {
        }

        [Fact]
        public void can_get_detailed_collection_statistics()
        {
            string strCollectionName = "Companies";
            using (var store = GetDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 20; i++)
                    {
                        bulk.Store(new Company { Name = "name" + i });
                    }
                }

                var detailedCollectionStats = store.Maintenance.Send(new GetDetailedCollectionStatisticsOperation());

                Assert.Equal(20, detailedCollectionStats.Collections[strCollectionName]);
                Assert.Equal(20, detailedCollectionStats.ExtendedCollectionDetails[strCollectionName].CountOfDocuments);
                Assert.True(detailedCollectionStats.ExtendedCollectionDetails[strCollectionName].Size.SizeInBytes > 0);
            }
        }
    }
}

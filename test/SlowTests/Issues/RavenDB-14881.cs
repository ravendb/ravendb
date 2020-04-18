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
        public void Receive_The_Number_Of_Documents_And_PhysicalSize_Using_The_CollectionStatistics_Operation()
        {
            using(var store = GetDocumentStore())
            {
                using(var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 50; i++)
                    {
                        bulk.Store(new Company { Name = "company" + i });
                    }
                }

                var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());

                Assert.Equal(50, collectionStatistics.Collections["Companies"].CountOfDocuments);
                Assert.True(collectionStatistics.Collections["Companies"].Size.SizeInBytes > 0);
            }
        }
    }
}

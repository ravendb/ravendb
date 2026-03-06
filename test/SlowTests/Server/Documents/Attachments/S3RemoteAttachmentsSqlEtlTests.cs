using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests.Voron.Util;
using Microsoft.Data.SqlClient;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Extensions;
using Raven.Server.SqlMigration;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL.Olap;
using SlowTests.Server.Documents.ETL.SQL;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments
{
    public class S3RemoteAttachmentsSqlEtlTests : SqlAwareTestBase
    {
        public S3RemoteAttachmentsSqlEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        private class Order
        {
            public Address Address { get; set; }
            public string Id { get; set; }
            public List<OrderLine> OrderLines { get; set; }
        }
        // ReSharper disable once ClassNeverInstantiated.Local
        private class OrderLine
        {
            public string Product { get; set; }
            public int Quantity { get; set; }
            public int Cost { get; set; }
        }
        // ReSharper disable once ClassNeverInstantiated.Local
        private class Address
        {
            public string City { get; set; }
        }

        [RequiresMsSqlRetryTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanSqlEtlRemoteAttachmentsToDestination(bool sqlRemote)
        {
            var s3Settings = Etl.GetS3Settings($"{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();

            try
            {
                using var store = GetDocumentStore();

                var conf = new RemoteAttachmentsConfiguration
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                   {
                       {
                           "conf-identifier", new RemoteAttachmentsDestinationConfiguration()
                           {
                               Disabled = false,
                               S3Settings = s3Settings,
                           }
                       }
                   },
                    CheckFrequencyInSec = 1,
                };
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRemoteAttachmentsOperation(conf));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var attachmentBytes = new byte[] { 1, 2, 3 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order());
                    await session.SaveChangesAsync();
                }

                using (var ms = new MemoryStream(attachmentBytes))
                {
                    var parameters1 = new StoreAttachmentParameters("test-attachment", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", sqlRemote ? DateTime.UtcNow : DateTime.UtcNow.AddDays(7)),
                        ContentType = "image/png"
                    };
                    var putOp1 = new PutAttachmentOperation("orders/1-A", parameters1);
                    store.Operations.Send(putOp1);
                }

                if (sqlRemote)
                {
                    int count = 0;
                    var remote = await WaitForValueAsync(async () =>
                    {
                        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);
                        count += await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                        return count;
                    }, 1, interval: 1000);
                    Assert.Equal(1, remote);
                }

                using (WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    SqlEtlTests.CreateRdbmsSchema(connectionString, @"
CREATE TABLE [dbo].[Orders]
(
    [Id] [nvarchar](50) NOT NULL,
    [Name] [nvarchar](255) NULL,
    [Pic] [varbinary](max) NULL
)
");
                    var etlDone = Etl.WaitForEtlToComplete(store);

                    SqlEtlTests.SetupSqlEtlInternal(store, Etl, connectionString, @"
var orderData = {
    Id: id(this),
    Name: this['@metadata']['@attachments'][0].Name,
    Pic: loadAttachment(this['@metadata']['@attachments'][0].Name)
};

loadToOrders(orderData);
");

                    await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());
                        }

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT Pic FROM Orders WHERE Id = 'orders/1-A'";

                            var sqlDataReader = await dbCommand.ExecuteReaderAsync();

                            Assert.True(sqlDataReader.Read());
                            var stream = sqlDataReader.GetStream(0);

                            var bytes = await stream.ReadDataAsync();

                            if (sqlRemote)
                            {
                                Assert.Equal([], bytes);
                            }
                            else
                            {
                                Assert.Equal(attachmentBytes, bytes);
                            }
                        }
                    }
                }
            }
            finally
            {
                await S3TestsHelper.DeleteObjects(s3Settings, prefix: $"{s3Settings.RemoteFolderName}", delimiter: string.Empty);
            }
        }
    }
}

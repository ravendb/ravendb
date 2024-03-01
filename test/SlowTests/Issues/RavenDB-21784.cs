using FastTests;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21784 : RavenTestBase
    {
        public RavenDB_21784(ITestOutputHelper output) : base(output)
        {
        }

        [RequiresMsSqlFact]
        [RavenFact(RavenTestCategory.Etl)]
        public void DeprecatedFactoryNameOfMySqlConnectionStringIsBeingReplacedDuringPut()
        {
            using (var store = GetDocumentStore())
            {
                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                    FactoryName = "MySql.Data.MySqlClient"
                };

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result1.RaftCommandIndex);


                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));
                Assert.Equal(sqlConnectionString.Name, record.SqlConnectionStrings["SqlConnectionString"].Name);
                Assert.Equal(sqlConnectionString.ConnectionString, record.SqlConnectionStrings["SqlConnectionString"].ConnectionString);
                Assert.Equal("MySqlConnector.MySqlConnectorFactory", record.SqlConnectionStrings["SqlConnectionString"].FactoryName);
            }
        }
    }
}



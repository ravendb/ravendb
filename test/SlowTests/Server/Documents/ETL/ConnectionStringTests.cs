using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Server.ServerWide.Context;
using SlowTests.Server.Documents.ETL.SQL;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class ConnectionStringTests : EtlTestBase
    {
        [Fact]
        public void CanAddAndRemoveConnectionStrings()
        {
            using (var store = GetDocumentStore())
            {
                var ravenConnectionString = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    Url = "http://127.0.0.1:8080",
                    Database = "Northwind",
                };
                store.Admin.Server.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString, store.Database));

                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = SqlEtlTests.GetConnectionString(store),
                };

                store.Admin.Server.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString, store.Database));

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record =  Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.Equal(ravenConnectionString.Name , record.RavenConnectionStrings["RavenConnectionString"].Name);
                Assert.Equal(ravenConnectionString.Url, record.RavenConnectionStrings["RavenConnectionString"].Url);
                Assert.Equal(ravenConnectionString.Database, record.RavenConnectionStrings["RavenConnectionString"].Database);

                Assert.True(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));
                Assert.Equal(sqlConnectionString.Name, record.SqlConnectionStrings["SqlConnectionString"].Name);
                Assert.Equal(sqlConnectionString.ConnectionString, record.SqlConnectionStrings["SqlConnectionString"].ConnectionString);

                store.Admin.Server.Send(new RemoveConnectionStringOperation<RavenConnectionString>(ravenConnectionString, store.Database));
                store.Admin.Server.Send(new RemoveConnectionStringOperation<SqlConnectionString>(sqlConnectionString, store.Database));

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.False(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.False(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));

            }
        }

        [Fact]
        public void CanUpdateConnectionStrings()
        {
            using (var store = GetDocumentStore())
            {
                var ravenConnectionString = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    Url = "http://127.0.0.1:8080",
                    Database = "Northwind",
                };
                store.Admin.Server.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString, store.Database));

                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = SqlEtlTests.GetConnectionString(store),
                };

                store.Admin.Server.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString, store.Database));

                //update url
                ravenConnectionString.Url = "http://127.0.0.1:8081";
                store.Admin.Server.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString, store.Database));
                
                //update name : need to remove the old entry
                store.Admin.Server.Send(new RemoveConnectionStringOperation<SqlConnectionString>(sqlConnectionString, store.Database));
                sqlConnectionString.Name = "New-Name";
                store.Admin.Server.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString, store.Database));

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.Equal("http://127.0.0.1:8081", record.RavenConnectionStrings["RavenConnectionString"].Url);

                Assert.False(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));
                Assert.True(record.SqlConnectionStrings.ContainsKey("New-Name"));
                Assert.Equal(sqlConnectionString.ConnectionString, record.SqlConnectionStrings["New-Name"].ConnectionString);
            }
        }

        [Fact]
        public void CanGetAllConnectionStrings()
        {
            using (var store = GetDocumentStore())
            {
                var ravenConnectionStrings = new List<RavenConnectionString>();
                var sqlConnectionStrings = new List<SqlConnectionString>();
                for (var i = 0; i < 5; i++)
                {
                    var ravenConnectionStr = new RavenConnectionString()
                    {
                        Name = $"RavenConnectionString{i}",
                        Url = $"http://127.0.0.1:808{i}",
                        Database = "Northwind",
                    };
                    var sqlConnectionStr = new SqlConnectionString
                    {
                        Name = $"SqlConnectionString{i}",
                        ConnectionString = SqlEtlTests.GetConnectionString(store)
                    };

                    ravenConnectionStrings.Add(ravenConnectionStr);
                    sqlConnectionStrings.Add(sqlConnectionStr);

                    store.Admin.Server.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr, store.Database));
                    store.Admin.Server.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionStr, store.Database));
                }

                var result = store.Admin.Server.Send(new GetConnectionStringsOperation(store.Database));
                Assert.NotNull(result.SqlConnectionStrings);
                Assert.NotNull(result.RavenConnectionStrings);

                for (var i = 0; i < 5; i++)
                {
                    result.SqlConnectionStrings.TryGetValue($"SqlConnectionString{i}", out var sql);
                    Assert.Equal(sql?.ConnectionString, sqlConnectionStrings[i].ConnectionString);

                    result.RavenConnectionStrings.TryGetValue($"RavenConnectionString{i}", out var raven);
                    Assert.Equal(raven?.Url, ravenConnectionStrings[i].Url);
                    Assert.Equal(raven?.Database, ravenConnectionStrings[i].Database);
                }
            }
        }
    }
}
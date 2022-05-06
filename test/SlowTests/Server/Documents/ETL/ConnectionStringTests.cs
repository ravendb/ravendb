using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class ConnectionStringTests : EtlTestBase
    {
        public ConnectionStringTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanAddAndRemoveConnectionStrings()
        {
            using (var store = GetDocumentStore())
            {
                var ravenConnectionString = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" },
                    Database = "Northwind",
                };
                var result0 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result0.RaftCommandIndex);

                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = MssqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                };

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result1.RaftCommandIndex);
                
                var elasticSearchConnectionString = new ElasticSearchConnectionString
                {
                    Name = "ElasticSearchConnectionString",
                    Nodes = new[]{"http://127.0.0.1:8080" },
                };

                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticSearchConnectionString>(elasticSearchConnectionString));
                Assert.NotNull(result2.RaftCommandIndex);
                
                var queueConnectionString = new QueueConnectionString
                {
                    Name = "ElasticSearchConnectionString",
                    Url = "http://127.0.0.1:8080"
                };

                var resultQueue = store.Maintenance.Send(new PutConnectionStringOperation<QueueConnectionString>(queueConnectionString));
                Assert.NotNull(resultQueue.RaftCommandIndex);

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record =  Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.Equal(ravenConnectionString.Name , record.RavenConnectionStrings["RavenConnectionString"].Name);
                Assert.Equal(ravenConnectionString.TopologyDiscoveryUrls, record.RavenConnectionStrings["RavenConnectionString"].TopologyDiscoveryUrls);
                Assert.Equal(ravenConnectionString.Database, record.RavenConnectionStrings["RavenConnectionString"].Database);

                Assert.True(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));
                Assert.Equal(sqlConnectionString.Name, record.SqlConnectionStrings["SqlConnectionString"].Name);
                Assert.Equal(sqlConnectionString.ConnectionString, record.SqlConnectionStrings["SqlConnectionString"].ConnectionString);
                
                Assert.True(record.ElasticSearchConnectionStrings.ContainsKey("ElasticSearchConnectionString"));
                Assert.Equal(elasticSearchConnectionString.Name , record.ElasticSearchConnectionStrings["ElasticSearchConnectionString"].Name);
                Assert.Equal(elasticSearchConnectionString.Nodes, record.ElasticSearchConnectionStrings["ElasticSearchConnectionString"].Nodes);
                
                Assert.True(record.QueueConnectionStrings.ContainsKey("QueueConnectionString"));
                Assert.Equal(queueConnectionString.Name , record.QueueConnectionStrings["QueueConnectionString"].Name);
                Assert.Equal(queueConnectionString.Url, record.QueueConnectionStrings["QueueConnectionString"].Url);

                var result3 = store.Maintenance.Send(new RemoveConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result3.RaftCommandIndex);
                var result4 = store.Maintenance.Send(new RemoveConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result4.RaftCommandIndex);
                var result5 = store.Maintenance.Send(new RemoveConnectionStringOperation<ElasticSearchConnectionString>(elasticSearchConnectionString));
                Assert.NotNull(result5.RaftCommandIndex);
                var result6 = store.Maintenance.Send(new RemoveConnectionStringOperation<QueueConnectionString>(queueConnectionString));
                Assert.NotNull(result6.RaftCommandIndex);

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.False(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.False(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));
                Assert.False(record.ElasticSearchConnectionStrings.ContainsKey("ElasticSearchConnectionString"));
                Assert.False(record.QueueConnectionStrings.ContainsKey("QueueConnectionString"));

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
                    TopologyDiscoveryUrls = new[]{"http://127.0.0.1:8080" },
                    Database = "Northwind",
                };
                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result1.RaftCommandIndex);

                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = MssqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                };

                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result2.RaftCommandIndex);
                
                var elasticSearchConnectionString = new ElasticSearchConnectionString
                {
                    Name = "ElasticConnectionString",
                    Nodes = new[]{"http://127.0.0.1:8080" },
                };
                
                var result3 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticSearchConnectionString>(elasticSearchConnectionString));
                Assert.NotNull(result3.RaftCommandIndex);

                //update url
                ravenConnectionString.TopologyDiscoveryUrls = new[]{"http://127.0.0.1:8081"};
                var result4 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result4.RaftCommandIndex);
                
                //update name : need to remove the old entry
                var result5 = store.Maintenance.Send(new RemoveConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result5.RaftCommandIndex);
                sqlConnectionString.Name = "New-Name";
                var result6 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result6.RaftCommandIndex);
                
                //update url
                elasticSearchConnectionString.Nodes = new[]{"http://127.0.0.1:8081"};
                var result7 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticSearchConnectionString>(elasticSearchConnectionString));
                Assert.NotNull(result7.RaftCommandIndex);

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.Equal("http://127.0.0.1:8081", record.RavenConnectionStrings["RavenConnectionString"].TopologyDiscoveryUrls.First());
                
                Assert.True(record.ElasticSearchConnectionStrings.ContainsKey("ElasticConnectionString"));
                Assert.Equal("http://127.0.0.1:8081", record.ElasticSearchConnectionStrings["ElasticConnectionString"].Nodes.First());

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
                var elasticSearchConnectionStrings = new List<ElasticSearchConnectionString>();
                for (var i = 0; i < 5; i++)
                {
                    var ravenConnectionStr = new RavenConnectionString()
                    {
                        Name = $"RavenConnectionString{i}",
                        TopologyDiscoveryUrls = new[] { $"http://127.0.0.1:808{i}" },
                        Database = "Northwind",
                    };
                    var sqlConnectionStr = new SqlConnectionString
                    {
                        Name = $"SqlConnectionString{i}",
                        ConnectionString = MssqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}"
                    };
                    var elasticConnectionStr = new ElasticSearchConnectionString
                    {
                        Name = $"ElasticConnectionString{i}",
                        Nodes = new[] { $"http://127.0.0.1:808{i}" },
                    };

                    ravenConnectionStrings.Add(ravenConnectionStr);
                    sqlConnectionStrings.Add(sqlConnectionStr);
                    elasticSearchConnectionStrings.Add(elasticConnectionStr);

                    var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr));
                    Assert.NotNull(result1.RaftCommandIndex);
                    var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionStr));
                    Assert.NotNull(result2.RaftCommandIndex);
                    var result3 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticSearchConnectionString>(elasticConnectionStr));
                    Assert.NotNull(result3.RaftCommandIndex);
                }

                var result = store.Maintenance.Send(new GetConnectionStringsOperation());
                Assert.NotNull(result.SqlConnectionStrings);
                Assert.NotNull(result.RavenConnectionStrings);
                Assert.NotNull(result.ElasticSearchConnectionStrings);

                for (var i = 0; i < 5; i++)
                {
                    result.SqlConnectionStrings.TryGetValue($"SqlConnectionString{i}", out var sql);
                    Assert.Equal(sql?.ConnectionString, sqlConnectionStrings[i].ConnectionString);
                    
                    result.ElasticSearchConnectionStrings.TryGetValue($"ElasticConnectionString{i}", out var elastic);
                    Assert.Equal(elastic?.Nodes, elasticSearchConnectionStrings[i].Nodes);

                    result.RavenConnectionStrings.TryGetValue($"RavenConnectionString{i}", out var raven);
                    Assert.Equal(raven?.TopologyDiscoveryUrls, ravenConnectionStrings[i].TopologyDiscoveryUrls);
                    Assert.Equal(raven?.Database, ravenConnectionStrings[i].Database);
                }
            }
        }

        [Fact]
        public void CanGetConnectionStringByName()
        {
            using (var store = GetDocumentStore())
            {
                var ravenConnectionStr = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                };
                var sqlConnectionStr = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = MssqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}"
                };
                var elasticConnectionStr = new ElasticSearchConnectionString
                {
                    Name = "ElasticConnectionString",
                    Nodes = new[] { "http://127.0.0.1:8080" },
                };

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr));
                Assert.NotNull(result1.RaftCommandIndex);
                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionStr));
                Assert.NotNull(result2.RaftCommandIndex);
                var result3 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticSearchConnectionString>(elasticConnectionStr));
                Assert.NotNull(result3.RaftCommandIndex);

                var resultSql = store.Maintenance.Send(new GetConnectionStringsOperation(connectionStringName: sqlConnectionStr.Name, type: sqlConnectionStr.Type));
                Assert.True(resultSql.SqlConnectionStrings.Count > 0);
                Assert.True(resultSql.RavenConnectionStrings.Count == 0);
                Assert.True(resultSql.ElasticSearchConnectionStrings.Count == 0);
                
                var resultElastic = store.Maintenance.Send(new GetConnectionStringsOperation(connectionStringName: elasticConnectionStr.Name, type: elasticConnectionStr.Type));
                Assert.True(resultElastic.SqlConnectionStrings.Count == 0);
                Assert.True(resultElastic.RavenConnectionStrings.Count == 0);
                Assert.True(resultElastic.ElasticSearchConnectionStrings.Count > 0);
            }
        }
    }
}

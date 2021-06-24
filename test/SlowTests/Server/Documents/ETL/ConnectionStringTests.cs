using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Elasticsearch;
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
                
                var elasticsearchConnectionString = new ElasticsearchConnectionString
                {
                    Name = "ElasticsearchConnectionString",
                    Nodes = new[]{"http://127.0.0.1:8080" },
                };

                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticsearchConnectionString>(elasticsearchConnectionString));
                Assert.NotNull(result2.RaftCommandIndex);

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
                
                Assert.True(record.ElasticsearchConnectionStrings.ContainsKey("ElasticsearchConnectionString"));
                Assert.Equal(elasticsearchConnectionString.Name , record.ElasticsearchConnectionStrings["ElasticsearchConnectionString"].Name);
                Assert.Equal(elasticsearchConnectionString.Nodes, record.ElasticsearchConnectionStrings["ElasticsearchConnectionString"].Nodes);

                var result3 = store.Maintenance.Send(new RemoveConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result3.RaftCommandIndex);
                var result4 = store.Maintenance.Send(new RemoveConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result4.RaftCommandIndex);
                var result5 = store.Maintenance.Send(new RemoveConnectionStringOperation<ElasticsearchConnectionString>(elasticsearchConnectionString));
                Assert.NotNull(result5.RaftCommandIndex);

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.False(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.False(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));
                Assert.False(record.ElasticsearchConnectionStrings.ContainsKey("ElasticsearchConnectionString"));

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
                
                var elasticConnectionString = new ElasticsearchConnectionString
                {
                    Name = "ElasticConnectionString",
                    Nodes = new[]{"http://127.0.0.1:8080" },
                };
                
                var result3 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticsearchConnectionString>(elasticConnectionString));
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
                elasticConnectionString.Nodes = new[]{"http://127.0.0.1:8081"};
                var result7 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticsearchConnectionString>(elasticConnectionString));
                Assert.NotNull(result7.RaftCommandIndex);

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.Equal("http://127.0.0.1:8081", record.RavenConnectionStrings["RavenConnectionString"].TopologyDiscoveryUrls.First());
                
                Assert.True(record.ElasticsearchConnectionStrings.ContainsKey("ElasticConnectionString"));
                Assert.Equal("http://127.0.0.1:8081", record.ElasticsearchConnectionStrings["ElasticConnectionString"].Nodes.First());

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
                var elasticConnectionStrings = new List<ElasticsearchConnectionString>();
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
                    var elasticConnectionStr = new ElasticsearchConnectionString
                    {
                        Name = $"ElasticConnectionString{i}",
                        Nodes = new[] { $"http://127.0.0.1:808{i}" },
                    };

                    ravenConnectionStrings.Add(ravenConnectionStr);
                    sqlConnectionStrings.Add(sqlConnectionStr);
                    elasticConnectionStrings.Add(elasticConnectionStr);

                    var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr));
                    Assert.NotNull(result1.RaftCommandIndex);
                    var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionStr));
                    Assert.NotNull(result2.RaftCommandIndex);
                    var result3 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticsearchConnectionString>(elasticConnectionStr));
                    Assert.NotNull(result3.RaftCommandIndex);
                }

                var result = store.Maintenance.Send(new GetConnectionStringsOperation());
                Assert.NotNull(result.SqlConnectionStrings);
                Assert.NotNull(result.RavenConnectionStrings);
                Assert.NotNull(result.ElasticsearchConnectionStrings);

                for (var i = 0; i < 5; i++)
                {
                    result.SqlConnectionStrings.TryGetValue($"SqlConnectionString{i}", out var sql);
                    Assert.Equal(sql?.ConnectionString, sqlConnectionStrings[i].ConnectionString);
                    
                    result.ElasticsearchConnectionStrings.TryGetValue($"ElasticConnectionString{i}", out var elastic);
                    Assert.Equal(elastic?.Nodes, elasticConnectionStrings[i].Nodes);

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
                var elasticConnectionStr = new ElasticsearchConnectionString
                {
                    Name = "ElasticConnectionString",
                    Nodes = new[] { "http://127.0.0.1:8080" },
                };

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr));
                Assert.NotNull(result1.RaftCommandIndex);
                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionStr));
                Assert.NotNull(result2.RaftCommandIndex);
                var result3 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticsearchConnectionString>(elasticConnectionStr));
                Assert.NotNull(result3.RaftCommandIndex);

                var resultSql = store.Maintenance.Send(new GetConnectionStringsOperation(connectionStringName: sqlConnectionStr.Name, type: sqlConnectionStr.Type));
                Assert.True(resultSql.SqlConnectionStrings.Count > 0);
                Assert.True(resultSql.RavenConnectionStrings.Count == 0);
                Assert.True(resultSql.ElasticsearchConnectionStrings.Count == 0);
                
                var resultElastic = store.Maintenance.Send(new GetConnectionStringsOperation(connectionStringName: elasticConnectionStr.Name, type: elasticConnectionStr.Type));
                Assert.True(resultElastic.SqlConnectionStrings.Count == 0);
                Assert.True(resultElastic.RavenConnectionStrings.Count == 0);
                Assert.True(resultElastic.ElasticsearchConnectionStrings.Count > 0);
            }
        }
    }
}

using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
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

        [RequiresMsSqlFact]
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
                    ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                    FactoryName = "Npgsql"
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
                    Name = "QueueEtlConnectionString-Kafka",
                    BrokerType = QueueBrokerType.Kafka,
                    KafkaConnectionSettings = new KafkaConnectionSettings(){BootstrapServers = "localhost:9092" }
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
                
                Assert.True(record.QueueConnectionStrings.ContainsKey("QueueEtlConnectionString-Kafka"));
                Assert.Equal(queueConnectionString.Name , record.QueueConnectionStrings["QueueEtlConnectionString-Kafka"].Name);
                Assert.Equal(queueConnectionString.KafkaConnectionSettings.BootstrapServers, record.QueueConnectionStrings["QueueEtlConnectionString-Kafka"].KafkaConnectionSettings.BootstrapServers);

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
                Assert.False(record.QueueConnectionStrings.ContainsKey("QueueEtlConnectionString-Kafka"));

            }
        }

        [RequiresMsSqlFact]
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
                    ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                    FactoryName = "Npgsql"
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

        [RequiresMsSqlFact]
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
                        ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                        FactoryName = "Npgsql"
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
                    ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                    FactoryName = "Npgsql"
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

        [Fact]
        public void CannotAddSqlConnectionStringWithInvalidFactoryName()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<BadRequestException>(() => store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
                {
                    Name = "Invalid Factory Connection String",
                    ConnectionString = "some-connection-string-that-doesnt-matter",
                    FactoryName = "Invalid.Factory.4.20-final.stable"
                }))); 
                Assert.Contains("Invalid connection string configuration. Errors: Unsupported factory 'Invalid.Factory.4.20-final.stable'", e.Message);
                
                e = Assert.Throws<BadRequestException>(()=>store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
                    {
                        Name = "Not-supported Factory Connection String",
                        ConnectionString = "some-connection-string-that-doesnt-matter",
                        FactoryName = "System.Data.OleDb"
                    }))); 
                Assert.Contains("Raven.Client.Exceptions.BadRequestException: Invalid connection string configuration. Errors: Factory 'System.Data.OleDb' is not implemented yet.", e.Message);
            }
        }

        [Fact]
        public void ConnectionStringsAuditJsonDoesntIncludeCredentials()
        {
            var sqlConnectionString = new SqlConnectionString
            {
                Name = "SqlConnectionString",
                ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + ";Initial Catalog=0x901507",
                FactoryName = "Npgsql"
            };

            var elasticSearchConnectionString = new ElasticSearchConnectionString
            {
                Name = "ElasticSearchConnectionString",
                Nodes = ["http://127.0.0.1:8080"],
            };

            var queueConnectionString = new QueueConnectionString
            {
                Name = "QueueEtlConnectionString-Kafka",
                BrokerType = QueueBrokerType.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings {BootstrapServers = "localhost:9092" },
                RabbitMqConnectionSettings = new RabbitMqConnectionSettings {ConnectionString = "rabbitmq:here"}
            };

            var olapConnectionString = new OlapConnectionString
            {
                AzureSettings = new AzureSettings { AccountKey = "q", AccountName = "w", RemoteFolderName = string.Empty, StorageContainer = "322" },
                FtpSettings = new FtpSettings { Url = string.Empty, },
                GlacierSettings = new GlacierSettings { RemoteFolderName = string.Empty, AwsAccessKey = "q", AwsSecretKey = "w" },
                GoogleCloudSettings = new GoogleCloudSettings { RemoteFolderName = string.Empty, BucketName = "b", GoogleCredentialsJson = "{}" },
                S3Settings = new S3Settings { RemoteFolderName = string.Empty, AwsAccessKey = "q", AwsSecretKey = "w" },
                Name = "lmao"
            };

            var sqlConnectionStringAuditJson = sqlConnectionString.ToAuditJson();
            var elasticSearchConnectionStringAuditJson = elasticSearchConnectionString.ToAuditJson();
            var queueConnectionStringAuditJson = queueConnectionString.ToAuditJson();
            var olapConnectionStringAuditJson = olapConnectionString.ToAuditJson();

            var kafkaConnectionSettingsAuditJson = (DynamicJsonValue)queueConnectionStringAuditJson[nameof(KafkaConnectionSettings)];
            var rabbitmqConnectionSettingsAuditJson= (DynamicJsonValue)queueConnectionStringAuditJson[nameof(RabbitMqConnectionSettings)];

            var azureSettingsAuditJson = (DynamicJsonValue)olapConnectionStringAuditJson[nameof(AzureSettings)];
            var ftpSettingsAuditJson = (DynamicJsonValue)olapConnectionStringAuditJson[nameof(FtpSettings)];
            var glacierSettingsAuditJson = (DynamicJsonValue)olapConnectionStringAuditJson[nameof(GlacierSettings)];
            var googleCloudSettingsAuditJson = (DynamicJsonValue)olapConnectionStringAuditJson[nameof(GoogleCloudSettings)];
            var s3SettingsAuditJson = (DynamicJsonValue)olapConnectionStringAuditJson[nameof(S3Settings)];
            
            Assert.False(sqlConnectionStringAuditJson.Properties.Select(x => x.Name).Contains("ConnectionString"));
            Assert.False(elasticSearchConnectionStringAuditJson.Properties.Select(x => x.Name).Contains("Authentication"));
            
            Assert.False(kafkaConnectionSettingsAuditJson.Properties.Select(x => x.Name).Contains("ConnectionOptions"));
            
            Assert.False(rabbitmqConnectionSettingsAuditJson.Properties.Select(x => x.Name).Contains("ConnectionString"));
                       
            Assert.False(azureSettingsAuditJson.Properties.Select(x => x.Name).Contains("SasToken"));
            Assert.False(azureSettingsAuditJson.Properties.Select(x => x.Name).Contains("AccountKey"));
            
            Assert.False(ftpSettingsAuditJson.Properties.Select(x => x.Name).Contains("Password"));
            Assert.False(ftpSettingsAuditJson.Properties.Select(x => x.Name).Contains("CertificateAsBase64"));
            
            Assert.False(glacierSettingsAuditJson.Properties.Select(x => x.Name).Contains("AwsSessionToken"));
            Assert.False(glacierSettingsAuditJson.Properties.Select(x => x.Name).Contains("AwsSecretKey"));
            Assert.False(glacierSettingsAuditJson.Properties.Select(x => x.Name).Contains("AwsAccessKey"));
            
            Assert.False(googleCloudSettingsAuditJson.Properties.Select(x => x.Name).Contains("GoogleCredentialsJson"));
            
            Assert.False(s3SettingsAuditJson.Properties.Select(x => x.Name).Contains("AwsSessionToken"));
            Assert.False(s3SettingsAuditJson.Properties.Select(x => x.Name).Contains("AwsSecretKey"));
            Assert.False(s3SettingsAuditJson.Properties.Select(x => x.Name).Contains("AwsAccessKey"));
        }
    }
}

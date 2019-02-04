using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations.Certificates;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_12809 : EtlTestBase
    {
        [Fact]
        public void Can_setup_etl_from_encrypted_to_non_encrypted_db()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = Path.GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            using (var src = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                ModifyDatabaseName = s => dbName,
            }))
            {
                AddEtl(src, new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myFirstEtl",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Collections =
                            {
                                "Users"
                            },
                            Script = "loadToUsers(this)",
                            Name = "a"
                        }
                    },
                    AllowEtlOnNonEncryptedChannel = true
                }, new RavenConnectionString()
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] {"http://127.0.0.1:8080"},
                    Database = "Northwind",
                });

                var db = GetDatabase(src.Database).Result;

                Assert.Equal(1, db.EtlLoader.Processes.Length);
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_13220 : RavenTestBase
    {
        public RavenDB_13220(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Etl_from_encrypted_to_non_encrypted_db_will_work(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
#pragma warning disable CA1416 // Validate platform compatibility
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            options.AdminCertificate = adminCert;
            options.ClientCertificate = adminCert;
            options.ModifyDatabaseRecord += record => record.Encrypted = true;
            options.ModifyDatabaseName = s => dbName;

            using (var src = GetDocumentStore(options))
            using (var dstServer = GetNewServer())
            using (var dest = GetDocumentStore(new Options()
            {
                Server = dstServer
            }))
            {
                Etl.AddEtl(src, new RavenEtlConfiguration()
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
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                const string docId = "users/1-A";
                var db = await Etl.GetDatabaseFor(src, docId);

                Assert.Equal(1, db.EtlLoader.Processes.Length);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, docId);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(docId);

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Etl_from_encrypted_to_encrypted_db_will_work_even_if_AllowEtlOnNonEncryptedChannel_is_set(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var srcDbName = GetDatabaseName();
            var dstDbName = GetDatabaseName();

            var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var srcBase64Key = Convert.ToBase64String(buffer);
            var dstBase64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
#pragma warning disable CA1416 // Validate platform compatibility
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(srcBase64Key, srcDbName, true);
            Server.ServerStore.PutSecretKey(dstBase64Key, dstDbName, true);

            options.AdminCertificate = adminCert;
            options.ClientCertificate = adminCert;
            options.ModifyDatabaseRecord += record => record.Encrypted = true;
            options.ModifyDatabaseName = _ => srcDbName;

            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                ModifyDatabaseName = s => dstDbName,
            }))
            {
                Etl.AddEtl(src, new RavenEtlConfiguration()
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
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                const string docId = "users/1-A";
                var db = await Etl.GetDatabaseFor(src, docId);

                Assert.Equal(1, db.EtlLoader.Processes.Length);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, docId);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(docId);

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);
                }
            }
        }
    }
}

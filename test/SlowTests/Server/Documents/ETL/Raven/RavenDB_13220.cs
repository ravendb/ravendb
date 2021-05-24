using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Logging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_13220 : EtlTestBase
    {
        public RavenDB_13220(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Etl_from_encrypted_to_non_encrypted_db_will_work()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

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
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            using (var src = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                ModifyDatabaseName = s => dbName,
            }))
            using (var dstServer = GetNewServer())
            using (var dest = GetDocumentStore(new Options()
            {
                Server = dstServer
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
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                var db = GetDatabase(src.Database).Result;

                Assert.Equal(1, db.EtlLoader.Processes.Length);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    });

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);
                }
            }
        }

        [Fact]
        public void Etl_from_encrypted_to_encrypted_db_will_work_even_if_AllowEtlOnNonEncryptedChannel_is_set()
        {
            using var socket = new DummyWebSocket();
            var _ = LoggingSource.Instance.Register(socket, new LoggingSource.WebSocketContext(), CancellationToken.None);

            var certificates = SetupServerAuthentication();
            var srcDbName = GetDatabaseName();
            var dstDbName = GetDatabaseName();

            var adminCert = RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

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
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            try
            {
                Server.ServerStore.PutSecretKey(srcBase64Key, srcDbName, true);
                Server.ServerStore.PutSecretKey(dstBase64Key, dstDbName, true);
            }
            catch
            {
                if (Context.TestOutput != null)
                {
                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        var licenseStatus = context.ReadObject(Server.ServerStore.LicenseManager.LicenseStatus.ToJson(), "LicenseStatus");
                        Context.TestOutput.WriteLine(licenseStatus.ToString());
                    }
                    Server.ServerStore.LicenseManager.TryActivateLicense(false);
                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        var licenseStatus = context.ReadObject(Server.ServerStore.LicenseManager.LicenseStatus.ToJson(), "LicenseStatus");
                        Context.TestOutput.WriteLine(licenseStatus.ToString());
                    }
                    Context.TestOutput.WriteLine(socket.CloseAndGetLogsAsync().Result);
                }
                throw;
            }

            using (var src = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                ModifyDatabaseName = s => srcDbName,
            }))
            using (var dest = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                ModifyDatabaseName = s => dstDbName,
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
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                var db = GetDatabase(src.Database).Result;

                Assert.Equal(1, db.EtlLoader.Processes.Length);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    });

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);
                }
            }
        }
    }
}

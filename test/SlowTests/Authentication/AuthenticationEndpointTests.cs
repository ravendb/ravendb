using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication;

public class AuthenticationEndpointTests : RavenTestBase
{
    public AuthenticationEndpointTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void CanRestrictExternalScriptUsageForNonClusterAdmin(Options options)
    {
        var customerSettings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Security.RestrictExternalScriptUsageForNonClusterAdmin)] = "true"
        };
        var certificates = AuthenticationBasicTests.SetupServerAuthentication(Certificates, customerSettings);
        var dbName = GetDatabaseName();
        var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
        var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

        options.AdminCertificate = adminCert;
        options.ClientCertificate = userCert;
        options.ModifyDatabaseName = _ => dbName;

        using (var store = GetDocumentStore(options))
        {
            var configuration = new PeriodicBackupConfiguration
            {
                FullBackupFrequency = "0 0 * * *",
                LocalSettings = new LocalSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                },
            };

            var error = Assert.Throws<RavenException>(() => store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration)));
            Assert.Contains("an external script is not allowed for non cluster admins", error.Message);

            configuration = new PeriodicBackupConfiguration
            {
                FullBackupFrequency = "0 0 * * *",
                S3Settings = new S3Settings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                },
            };

            error = Assert.Throws<RavenException>(() => store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration)));
            Assert.Contains("an external script is not allowed for non cluster admins", error.Message);

            configuration = new PeriodicBackupConfiguration
            {
                FullBackupFrequency = "0 0 * * *",
                AzureSettings = new AzureSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                }
            };

            error = Assert.Throws<RavenException>(() => store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration)));
            Assert.Contains("an external script is not allowed for non cluster admins", error.Message);

            configuration = new PeriodicBackupConfiguration
            {
                FullBackupFrequency = "0 0 * * *",
                GlacierSettings = new GlacierSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                }
            };

            error = Assert.Throws<RavenException>(() => store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration)));
            Assert.Contains("an external script is not allowed for non cluster admins", error.Message);

            configuration = new PeriodicBackupConfiguration
            {
                FullBackupFrequency = "0 0 * * *",
                GoogleCloudSettings = new GoogleCloudSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                }
            };

            error = Assert.Throws<RavenException>(() => store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration)));
            Assert.Contains("an external script is not allowed for non cluster admins", error.Message);

            configuration = new PeriodicBackupConfiguration
            {
                FullBackupFrequency = "0 0 * * *",
                FtpSettings = new FtpSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                }
            };

            error = Assert.Throws<RavenException>(() => store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration)));
            Assert.Contains("an external script is not allowed for non cluster admins", error.Message);
        }
    }

    [RavenTheory(RavenTestCategory.Certificates)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void CanUpdateBackupWithRestrictExternalScriptUsageForNonClusterAdmin(Options options)
    {
        var customerSettings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Security.RestrictExternalScriptUsageForNonClusterAdmin)] = "true"
        };
        var certificates = AuthenticationBasicTests.SetupServerAuthentication(Certificates, customerSettings);
        var dbName = GetDatabaseName();
        var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
        var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

        options.AdminCertificate = adminCert;
        options.ClientCertificate = adminCert;
        options.ModifyDatabaseName = _ => dbName;

        using (var storeAdmin = GetDocumentStore(options))
        {
            var configuration = new PeriodicBackupConfiguration
            {
                FullBackupFrequency = "0 0 * * *",
                LocalSettings = new LocalSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                },
                S3Settings = new S3Settings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                },
                AzureSettings = new AzureSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                },
                GlacierSettings = new GlacierSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                },
                GoogleCloudSettings = new GoogleCloudSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                },
                FtpSettings = new FtpSettings
                {
                    GetBackupConfigurationScript = new GetBackupConfigurationScript
                    {
                        Exec = "test"
                    }
                }
            };

            var result = storeAdmin.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration));
            configuration.TaskId = result.TaskId;

            options.ClientCertificate = userCert;
            options.CreateDatabase = false;
            using (var store = GetDocumentStore(options))
            {
                configuration.IncrementalBackupFrequency = "0 30 * * *";
                store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration));

                configuration.LocalSettings.FolderPath = "c:\\";
                configuration.LocalSettings.GetBackupConfigurationScript = null;
                configuration.IncrementalBackupFrequency = "0 31 * * *";
                var error = Assert.Throws<RavenException>(() => store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration)));
                Assert.Contains("an external script is not allowed for non cluster admins", error.Message);
            }
        }
    }
}

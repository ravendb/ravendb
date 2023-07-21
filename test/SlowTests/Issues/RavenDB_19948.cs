using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Xunit;
using Xunit.Abstractions;
using Sparrow.Platform;
using Raven.Tests.Core.Utils.Entities;

namespace SlowTests.Issues
{
    public class RavenDB_19948 : RavenTestBase
    {
        public RavenDB_19948(ITestOutputHelper output) : base(output)
        {
        }

        /* should work:
         * One Time Backup.
         * attempt to post external configuration script by overriding local conf. destination
         * security clearance: ClusterAdmin , Operator
         */
        [Theory]
        [InlineData(SecurityClearance.ClusterAdmin)]
        [InlineData(SecurityClearance.Operator)]
        public void CanPostOneTimeBackupConfigurationScriptWithClusterAdminClearance(SecurityClearance sc)
        {
            var dbName = GetDatabaseName();
            var certificates = Certificates.SetupServerAuthentication();
            X509Certificate2 adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            X509Certificate2 clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate3.Value,
                new Dictionary<string, DatabaseAccess>(), sc);

            var path = NewDataPath(forceCreateDir: true);
            var scriptPath = GenerateConfigurationScript(path, out string command);

            using (var store = GetDocumentStore(new Options() { AdminCertificate = adminCertificate, ClientCertificate = clientCertificate, ModifyDatabaseName = s => dbName }))
            {
                using (var session = store.OpenSession(dbName))
                {
                    session.Store(new User() { Name = "Adeyemi" });
                    session.SaveChanges();
                }

                var operation = store.Maintenance.ForDatabase(dbName).Send(new BackupOperation(new BackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = path,
                        GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath }
                    }
                }));

                var backupResult = (BackupResult)operation.WaitForCompletion(TimeSpan.FromSeconds(30));
                Assert.NotEqual(0, backupResult.Documents.ReadCount);
                Assert.NotNull(backupResult.LocalBackup.BackupDirectory);
            }
        }

        /* shouldn't work:
         * One Time Backup.
         * attempt to post external configuration script by overriding local conf. destination
         * security clearance: ValidUser
         */
        [Fact]
        public void CannotPostOneTimeBackupConfigurationScriptWitValidUserClearance()
        {
            var dbName = GetDatabaseName();
            var certificates = Certificates.SetupServerAuthentication();
            X509Certificate2 adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            X509Certificate2 clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() { [dbName] = DatabaseAccess.Admin }, SecurityClearance.ValidUser);

            var path = NewDataPath(forceCreateDir: true);
            var scriptPath = GenerateConfigurationScript(path, out string command);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName
            }))
            {
                using (var session = store.OpenSession(dbName))
                {
                    session.Store(new User() { Name = "Adeyemi" });
                    session.SaveChanges();
                }

                Action act = () => store.Maintenance.ForDatabase(dbName).Send(new BackupOperation(new BackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = path,
                        GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath }
                    }
                })).WaitForCompletion(TimeSpan.FromSeconds(30));

                var exception = Assert.Throws<RavenException>(act);
                Assert.Contains(
                    $"Bad security clearance: '{RavenServer.AuthenticationStatus.Allowed}'. The current user does not have the necessary security clearance. This operation is only allowed for users with '{SecurityClearance.Operator}' or higher security clearance.",
                    exception.Message);
            }

        }

        /* should work:
         * Periodic Backup.
         * attempt to post external configuration script by overriding local conf. destination
         * security clearance: ClusterAdmin, Operator
         */
        [Theory]
        [InlineData(SecurityClearance.ClusterAdmin)]
        [InlineData(SecurityClearance.Operator)]
        public void CanPostPeriodicBackupConfigurationScriptWithClusterAdminClearance(SecurityClearance sc)
        {
            var dbName = GetDatabaseName();
            var certificates = Certificates.SetupServerAuthentication();
            X509Certificate2 adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            X509Certificate2 clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>(), sc);

            var path = NewDataPath(forceCreateDir: true);
            var scriptPath = GenerateConfigurationScript(path, out string command);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName
            }))
            {
                using (var session = store.OpenSession(dbName))
                {
                    session.Store(new User() { Name = "Adeyemi" });
                    session.SaveChanges();
                }

                var config = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", disabled: true);
                config.LocalSettings = new LocalSettings
                {
                    FolderPath = path,
                    GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath }
                };

                var result = store.Maintenance.ForDatabase(dbName).Send(new UpdatePeriodicBackupOperation(config));

                Assert.NotNull(result.RaftCommandIndex);

            }

        }

        /* shouldn't work:
         * Periodic Backup.
         * attempt to post external configuration script by overriding local conf. destination
         * security clearance: ValidUser
         */
        [Fact]
        public void CannotPostPeriodicBackupConfigurationScriptWitValidUserClearance()
        {
            var dbName = GetDatabaseName();
            var certificates = Certificates.SetupServerAuthentication();
            X509Certificate2 adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            X509Certificate2 clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() { [dbName] = DatabaseAccess.Admin }, SecurityClearance.ValidUser);

            var path = NewDataPath(forceCreateDir: true);
            var scriptPath = GenerateConfigurationScript(path, out string command);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName
            }))
            {
                using (var session = store.OpenSession(dbName))
                {
                    session.Store(new User() { Name = "Adeyemi" });
                    session.SaveChanges();
                }

                var config = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", disabled: true);
                config.LocalSettings = new LocalSettings
                {
                    FolderPath = path,
                    GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath }
                };

                Action act = () => store.Maintenance.ForDatabase(dbName).Send(new UpdatePeriodicBackupOperation(config));

                var exception = Assert.Throws<RavenException>(act);
                Assert.Contains(
                    $"Bad security clearance: '{RavenServer.AuthenticationStatus.Allowed}'. The current user does not have the necessary security clearance. This operation is only allowed for users with '{SecurityClearance.Operator}' or higher security clearance.",
                    exception.Message);
            }

        }

        /* should work:
       * Olap ETL Connection String.
       * attempt to post external connection string script by overriding local conf. destination
       * security clearance: ClusterAdmin, Operator
       */
        [Theory]
        [InlineData(SecurityClearance.ClusterAdmin)]
        [InlineData(SecurityClearance.Operator)]
        public void CanPostOlapConnectionStringScriptWithClusterAdminClearance(SecurityClearance sc)
        {

            var dbName = GetDatabaseName();
            TestCertificatesHolder certificates = Certificates.SetupServerAuthentication();
            X509Certificate2 adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            X509Certificate2 clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>(), sc);

            var path = NewDataPath(forceCreateDir: true);
            var scriptPath = GenerateConfigurationScript(path, out string command);

            using (var store = GetDocumentStore(new Options { AdminCertificate = adminCertificate, ClientCertificate = clientCertificate, ModifyDatabaseName = s => dbName }))
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
                var olapConnStr = new OlapConnectionString
                {
                    Name = "olap-cs",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = path,
                        GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath }
                    }
                };


                var result0 = store.Maintenance.Send(new PutConnectionStringOperation<OlapConnectionString>(olapConnStr));
                Assert.NotNull(result0.RaftCommandIndex);

                var result = store.Maintenance.Send(new GetConnectionStringsOperation(store.Database, ConnectionStringType.Olap));
                Assert.NotNull(result.RavenConnectionStrings);

            }
        }

        /* shouldn't work:
         * Olap ETL Connection String.
         * attempt to post external connection string script by overriding local conf. destination
         * security clearance: ValidUser
         */
        [Fact]
        public void CannotPostOlapConnectionStringScriptWitValidUserClearance()
        {

            var dbName = GetDatabaseName();
            TestCertificatesHolder certificates = Certificates.SetupServerAuthentication();
            X509Certificate2 adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            X509Certificate2 clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() { [dbName] = DatabaseAccess.Admin }, SecurityClearance.ValidUser);

            var path = NewDataPath(forceCreateDir: true);
            var scriptPath = GenerateConfigurationScript(path, out string command);

            using (var store = GetDocumentStore(new Options { AdminCertificate = adminCertificate, ClientCertificate = clientCertificate, ModifyDatabaseName = s => dbName }))
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
                var olapConnStr = new OlapConnectionString
                {
                    Name = "olap-cs",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = path,
                        GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath }
                    }
                };

                var exception = Assert.Throws<RavenException>(() =>
                    store.Maintenance.ForDatabase(dbName).Send(new PutConnectionStringOperation<OlapConnectionString>(olapConnStr)));
                Assert.Contains(
                    $"Bad security clearance: '{RavenServer.AuthenticationStatus.Allowed}'. The current user does not have the necessary security clearance. This operation is only allowed for users with '{SecurityClearance.Operator}' or higher security clearance.",
                    exception.Message);
            }
        }

        private static string GenerateConfigurationScript(string path, out string command)
        {
            var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var localSetting = new LocalSettings { FolderPath = path };
            var localSettingsString = JsonConvert.SerializeObject(localSetting);

            string script;
            if (PlatformDetails.RunningOnPosix)
            {
                command = "bash";
                script = $"#!/bin/bash\r\necho '{localSettingsString}'";
                File.WriteAllText(scriptPath, script);
                Process.Start("chmod", $"700 {scriptPath}");
            }
            else
            {
                command = "powershell";
                script = $"echo '{localSettingsString}'";
                File.WriteAllText(scriptPath, script);
            }

            return scriptPath;
        }

    }
}

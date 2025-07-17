using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Operations.Certificates;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24162 : RavenTestBase
    {
        public RavenDB_24162(ITestOutputHelper output) : base(output)
        {
        }

        private RetiredAttachmentsConfiguration SampleConfig() => new RetiredAttachmentsConfiguration
        {
            Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
            {
                {
                    "test", new RetiredAttachmentsDestinationConfiguration()
                    {
                        Disabled = true,
                        S3Settings = new S3Settings
                        {
                            BucketName = "test-bucket-does-not-exist", 
                            AwsRegionName = "us-west-2", 
                            AwsAccessKey = "AKIAFAKEKEY", 
                            AwsSecretKey = "FAKESECRET"
                        }, 
                        Identifier = "conf-identifier"
                    }
                }
            },
            RetireFrequencyInSec = 123456,
        };

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CannotGetRetireConfigWithValidUserPermission(Options options)
        {
            var certs = Certificates.SetupServerAuthentication();
            var db = GetDatabaseName();

            var adminCert = Certificates.RegisterClientCertificate(
                certs.ServerCertificate.Value,
                certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess> { [db] = DatabaseAccess.Admin },
                SecurityClearance.ClusterAdmin);

            var userCert = Certificates.RegisterClientCertificate(
                certs.ServerCertificate.Value,
                certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess> { [db] = DatabaseAccess.Read },
                SecurityClearance.ValidUser);

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => db;

            using (var store = GetDocumentStore(options))
            {
                AuthorizationException ex = Assert.Throws<AuthorizationException>(() =>
                    store.Maintenance.Send(new GetRetireAttachmentsConfigurationOperation()));
                Assert.NotNull(ex);

                Assert.Contains($"Forbidden access to {store.Database}", ex.Message);
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanGetRetireConfigWithValidAdminPermission(Options options)
        {
            var certs = Certificates.SetupServerAuthentication();
            var db = GetDatabaseName();

            var adminCert = Certificates.RegisterClientCertificate(
                certs.ServerCertificate.Value,
                certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess> { [db] = DatabaseAccess.Admin },
                SecurityClearance.ClusterAdmin);

            var userCert = Certificates.RegisterClientCertificate(
                certs.ServerCertificate.Value,
                certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess> { [db] = DatabaseAccess.Admin },
                SecurityClearance.ValidUser);

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => db;

            using (var store = GetDocumentStore(options))
            {
                var cfg = store.Maintenance.Send(new GetRetireAttachmentsConfigurationOperation());
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanAddRetireConfigWithDatabaseAdminPermission(Options options)
        {
            var certs = Certificates.SetupServerAuthentication();
            var db = GetDatabaseName();

            var adminCert = Certificates.RegisterClientCertificate(
                certs.ServerCertificate.Value,
                certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess> { [db] = DatabaseAccess.Admin },
                SecurityClearance.ClusterAdmin);

            var userCert = Certificates.RegisterClientCertificate(
                certs.ServerCertificate.Value,
                certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess> { [db] = DatabaseAccess.Admin });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => db;

            using (var store = GetDocumentStore(options))
            {
                var cfg = SampleConfig();

                store.Maintenance.Send(new ConfigureRetiredAttachmentsOperation(cfg));

                var returned = store.Maintenance.Send(new GetRetireAttachmentsConfigurationOperation());
                Assert.Equal(1, returned.Destinations.Count);
                Assert.True(returned.Destinations.First().Value.Disabled);
                var x = returned.RetireFrequencyInSec;
                Assert.Equal(123456, returned.RetireFrequencyInSec);
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CannotAddRetireConfigWithoutDatabaseAdminPermission(Options options)
        {
            var certs = Certificates.SetupServerAuthentication();
            var db = GetDatabaseName();

            var adminCert = Certificates.RegisterClientCertificate(
                certs.ServerCertificate.Value,
                certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess> { [db] = DatabaseAccess.Admin },
                SecurityClearance.ClusterAdmin);

            var userCert = Certificates.RegisterClientCertificate(
                certs.ServerCertificate.Value,
                certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess> { [db] = DatabaseAccess.ReadWrite },
                SecurityClearance.ValidUser);

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => db;

            using (var store = GetDocumentStore(options))
            {
                var cfg = SampleConfig();

                Assert.Throws<AuthorizationException>(() =>
                    store.Maintenance.Send(new ConfigureRetiredAttachmentsOperation(cfg)));
            }
        }
    }
}

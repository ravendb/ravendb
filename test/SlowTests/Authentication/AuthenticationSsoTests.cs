using System;
using System.Collections.Generic;
using System.Formats.Asn1;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication
{
    public class AuthenticationSsoTests : DisableParallelTestBase
    {
        public AuthenticationSsoTests(ITestOutputHelper output) : base(output)
        {
        }

        public static IEnumerable<object[]> AllProviders => new[]
        {
            new object[] { SsoProvider.Github,    null    },
            new object[] { SsoProvider.Google,    null    },
            new object[] { SsoProvider.Microsoft, null    },
            new object[] { SsoProvider.Windows,   "CORP"  },
        };

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void CanAccessDatabaseWithSsoAuthentication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "john@example.com";
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, ssoUserId);

            // Keep admin store alive so the database doesn't get cleaned up
            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite });

                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCert,
                    Conventions = new DocumentConventions
                    {
                        DisposeCertificate = false,
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }

                    using (var session = ssoStore.OpenSession())
                    {
                        var doc = session.Load<dynamic>("test/1");
                        Assert.NotNull(doc);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void SsoUserWithNoMatchingEntry_IsRejected()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, "unknown@example.com");

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
            }

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbName,
                Certificate = ssoUserCert,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void CertSignedByUnknownCA_WithSsoOid_IsRejected()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            // Create a completely separate SSO server key pair (not registered)
            using var unknownKey = RSA.Create(2048);
            var unknownCertReq = new CertificateRequest("CN=Unknown SSO", unknownKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            unknownCertReq.CertificateExtensions.Add(new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));
            unknownCertReq.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                new OidCollection { new Oid("1.3.6.1.5.5.7.3.2") }, false));
            var unknownSsoServerCert = unknownCertReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));

            var unknownSsoCerts = new SsoTestCertificates(
                unknownSsoServerCert,
                unknownKey,
                unknownSsoServerCert.GetPublicKeyPinningHash(),
                Convert.ToBase64String(unknownSsoServerCert.Export(X509ContentType.Cert)));

            var ssoUserCert = Certificates.CreateSsoUserCertificate(unknownSsoCerts, "hacker@example.com");

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbName,
                Certificate = ssoUserCert,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void SsoUserCannotAccessUnauthorizedDatabase()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameAuthorized = GetDatabaseName();
            var dbNameUnauthorized = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "limited@example.com";
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, ssoUserId);

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbNameAuthorized
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash,
                    new Dictionary<string, DatabaseAccess> { [dbNameAuthorized] = DatabaseAccess.ReadWrite });
            }

            using (var adminStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbNameUnauthorized
            }))
            {
            }

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbNameUnauthorized,
                Certificate = ssoUserCert,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public async Task SsoServerCertRenewal_UserEntriesStillWork()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "renewed@example.com";

            // Create a "renewed" SSO server cert using the same private key (different serial/thumbprint)
            var renewedSsoServerCert = CreateRenewedSsoServerCert(ssoCerts);
            var renewedPinningHash = renewedSsoServerCert.GetPublicKeyPinningHash();

            Assert.Equal(ssoCerts.SsoServerPublicKeyPinningHash, renewedPinningHash);

            var renewedSsoCerts = new SsoTestCertificates(
                renewedSsoServerCert,
                ssoCerts.SsoServerPrivateKey,
                renewedPinningHash,
                Convert.ToBase64String(renewedSsoServerCert.Export(X509ContentType.Cert)));

            var ssoUserCert = Certificates.CreateSsoUserCertificate(renewedSsoCerts, ssoUserId);

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite });

                // Register the renewed SSO server cert so the chain validation succeeds.
                // The user entry is pinned to the original public-key hash, which is identical to the
                // renewed cert's hash (same private key) — so existing user entries keep working.
                Certificates.RegisterSsoServerCert(certificates, renewedSsoCerts, "Renewed SSO Server Certificate");

                using (var ssoStore = new DocumentStore
                       {
                           Urls = new[] { Server.WebUrl },
                           Database = dbName,
                           Certificate = ssoUserCert,
                           Conventions = new DocumentConventions
                           {
                               DisposeCertificate = false,
                               DisableTopologyUpdates = true
                           }
                       }.Initialize())
                {
                    var requestExecutor = ssoStore.GetRequestExecutor();
                    await requestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(
                        new ServerNode { Url = Server.WebUrl, Database = dbName })
                    {
                        TimeoutInMs = 15000,
                        DebugTag = "sso-renewal-test"
                    });

                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Renewed" }, "test/1");
                        session.SaveChanges();
                    }

                    using (var session = ssoStore.OpenSession())
                    {
                        var doc = session.Load<dynamic>("test/1");
                        Assert.NotNull(doc);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void AllowAnySsoServer_GrantsAccessFromAnyRegisteredSsoServer()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCertsA = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoCertsB = CreateIndependentSsoServerCertificates();

            var ssoUserId = "anyserver@example.com";

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                // Register two different SSO servers
                Certificates.RegisterSsoServerCert(certificates, ssoCertsA);
                Certificates.RegisterSsoServerCert(certificates, ssoCertsB, "SSO Server B");

                // User entry allows any registered SSO server (no hash binding)
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoServerPublicKeyPinningHash: null, allowAnySsoServer: true,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite });

                // A cert signed by SSO server B should work even though the user entry has no explicit hash binding
                var ssoUserCertSignedByB = Certificates.CreateSsoUserCertificate(ssoCertsB, ssoUserId);

                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCertSignedByB,
                    Conventions = new DocumentConventions
                    {
                        DisposeCertificate = false,
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "TestB" }, "test/1");
                        session.SaveChanges();
                    }

                    using (var session = ssoStore.OpenSession())
                    {
                        var doc = session.Load<dynamic>("test/1");
                        Assert.NotNull(doc);
                    }
                }

                // A cert signed by SSO server A should also work
                var ssoUserCertSignedByA = Certificates.CreateSsoUserCertificate(ssoCertsA, ssoUserId);

                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCertSignedByA,
                    Conventions = new DocumentConventions
                    {
                        DisposeCertificate = false,
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        var doc = session.Load<dynamic>("test/1");
                        Assert.NotNull(doc);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void AllowAnySsoServer_StillRequiresRegisteredSsoServer()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCertsA = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "anyserver@example.com";

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                // Register SSO server A only
                Certificates.RegisterSsoServerCert(certificates, ssoCertsA);

                // User entry allows any registered SSO server
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoServerPublicKeyPinningHash: null, allowAnySsoServer: true,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite });
            }

            // Create an unregistered SSO server B and sign a user cert with it
            var ssoCertsB = CreateIndependentSsoServerCertificates();
            var ssoUserCertSignedByUnregistered = Certificates.CreateSsoUserCertificate(ssoCertsB, ssoUserId);

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbName,
                Certificate = ssoUserCertSignedByUnregistered,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                // Chain validation against known SSO servers will fail — no registered server signed this cert
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void SpecificSsoServer_WrongServer_IsRejected()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCertsA = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoCertsB = CreateIndependentSsoServerCertificates();

            var ssoUserId = "specificserver@example.com";

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                // Register both SSO servers
                Certificates.RegisterSsoServerCert(certificates, ssoCertsA);
                Certificates.RegisterSsoServerCert(certificates, ssoCertsB, "SSO Server B");

                // User entry is bound to SSO server A only (AllowAnySsoServer = false)
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCertsA.SsoServerPublicKeyPinningHash, allowAnySsoServer: false,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite });
            }

            // Sign the user cert with SSO server B — chain validates but hash binding fails
            var ssoUserCertSignedByB = Certificates.CreateSsoUserCertificate(ssoCertsB, ssoUserId);

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbName,
                Certificate = ssoUserCertSignedByB,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public async Task DisabledSsoUserEntry_IsRejected()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "disabled.from.start@example.com";
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, ssoUserId);
            var permissions = new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite };

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                var ssoUserThumbprint = Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash, permissions);

                // Disable immediately, before any successful auth
                var disableParams = new EditClientCertificateOperation.Parameters
                {
                    Thumbprint = ssoUserThumbprint,
                    Name = ssoUserId,
                    Permissions = permissions,
                    Clearance = SecurityClearance.ValidUser,
                    Disabled = true
                };
                await adminStore.Maintenance.Server.SendAsync(new EditClientCertificateOperation(disableParams));

                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCert,
                    Conventions = new DocumentConventions { DisposeCertificate = false, DisableTopologyUpdates = true }
                }.Initialize())
                {
                    Assert.Throws<AuthorizationException>(() =>
                    {
                        using (var session = ssoStore.OpenSession())
                        {
                            session.Store(new { Name = "Test" }, "test/1");
                            session.SaveChanges();
                        }
                    });
                }
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public async Task DisabledSsoUserEntry_CanBeReEnabled()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "reenable.test@example.com";
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, ssoUserId);
            var permissions = new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite };

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                var ssoUserThumbprint = Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash, permissions);

                var editParams = new EditClientCertificateOperation.Parameters
                {
                    Thumbprint = ssoUserThumbprint,
                    Name = ssoUserId,
                    Permissions = permissions,
                    Clearance = SecurityClearance.ValidUser,
                    Disabled = true
                };

                // Disable
                await adminStore.Maintenance.Server.SendAsync(new EditClientCertificateOperation(editParams));

                // Re-enable
                editParams.Disabled = false;
                await adminStore.Maintenance.Server.SendAsync(new EditClientCertificateOperation(editParams));

                // Verify access is restored after re-enabling
                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCert,
                    Conventions = new DocumentConventions { DisposeCertificate = false, DisableTopologyUpdates = true }
                }.Initialize())
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }

                    using (var session = ssoStore.OpenSession())
                    {
                        var doc = session.Load<dynamic>("test/1");
                        Assert.NotNull(doc);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public async Task CannotEditSsoServerCertificate()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);

                // Any edit to an SSO server certificate should be rejected
                var editParams = new EditClientCertificateOperation.Parameters
                {
                    Thumbprint = ssoCerts.SsoServerCert.Thumbprint,
                    Name = "SSO Server Certificate",
                    Permissions = new Dictionary<string, DatabaseAccess>(),
                    Clearance = SecurityClearance.ValidUser,
                    Disabled = true
                };

                await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    await adminStore.Maintenance.Server.SendAsync(new EditClientCertificateOperation(editParams));
                });
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void MalformedSsoUserIdExtension_IsRejected()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();

            // Bytes that are not a valid DER UTF8String — AsnReader.ReadCharacterString must reject them.
            var malformedExtensionData = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF };
            var ssoUserCert = CreateSsoUserCertificateWithRawExtension(ssoCerts, malformedExtensionData);

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
            }

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbName,
                Certificate = ssoUserCert,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void SsoUserIdExtension_WithTrailingBytes_IsRejected()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "trailing@example.com";

            // Valid UTF8String DER for the user id, followed by trailing bytes.
            // ThrowIfNotEmpty() must reject this even though the prefix parses correctly.
            var asnWriter = new AsnWriter(AsnEncodingRules.DER);
            asnWriter.WriteCharacterString(UniversalTagNumber.UTF8String, ssoUserId);
            var validBytes = asnWriter.Encode();
            var withTrailing = new byte[validBytes.Length + 4];
            Buffer.BlockCopy(validBytes, 0, withTrailing, 0, validBytes.Length);
            withTrailing[validBytes.Length] = 0x01;
            withTrailing[validBytes.Length + 1] = 0x02;
            withTrailing[validBytes.Length + 2] = 0x03;
            withTrailing[validBytes.Length + 3] = 0x04;

            var ssoUserCert = CreateSsoUserCertificateWithRawExtension(ssoCerts, withTrailing);

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                // Register the user entry too — the auth must still fail because the decoded id is empty,
                // not because the user is unknown.
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite });
            }

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbName,
                Certificate = ssoUserCert,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        [MemberData(nameof(AllProviders))]
        public void CanAccessDatabaseWithSsoAuthentication_AllProviders(SsoProvider provider, string domain)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = $"user-{provider}@example.com";
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, ssoUserId, provider, domain);

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite },
                    provider: provider, domain: domain);

                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCert,
                    Conventions = new DocumentConventions
                    {
                        DisposeCertificate = false,
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }

                    using (var session = ssoStore.OpenSession())
                    {
                        var doc = session.Load<dynamic>("test/1");
                        Assert.NotNull(doc);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        [MemberData(nameof(AllProviders))]
        public void SsoUserWithNoMatchingEntry_AllProviders_IsRejected(SsoProvider provider, string domain)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, $"unknown-{provider}@example.com", provider, domain);

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
            }

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbName,
                Certificate = ssoUserCert,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        [MemberData(nameof(AllProviders))]
        public void SpecificSsoServer_WrongServer_AllProviders_IsRejected(SsoProvider provider, string domain)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCertsA = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoCertsB = CreateIndependentSsoServerCertificates();

            var ssoUserId = $"specificserver-{provider}@example.com";

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCertsA);
                Certificates.RegisterSsoServerCert(certificates, ssoCertsB, "SSO Server B");

                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCertsA.SsoServerPublicKeyPinningHash, allowAnySsoServer: false,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite },
                    provider: provider, domain: domain);
            }

            // Sign the user cert with SSO server B — chain validates but hash binding fails
            var ssoUserCertSignedByB = Certificates.CreateSsoUserCertificate(ssoCertsB, ssoUserId, provider, domain);

            using (var ssoStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = dbName,
                Certificate = ssoUserCertSignedByB,
                Conventions = new DocumentConventions { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void Windows_SsoUser_WrongDomain_IsRejected()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "alice";

            // User cert claims a different domain than the one registered in the user entry.
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, ssoUserId, SsoProvider.Windows, domain: "OTHER");

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite },
                    provider: SsoProvider.Windows, domain: "CORP");

                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCert,
                    Conventions = new DocumentConventions { DisposeCertificate = false, DisableTopologyUpdates = true }
                }.Initialize())
                {
                    Assert.Throws<AuthorizationException>(() =>
                    {
                        using (var session = ssoStore.OpenSession())
                        {
                            session.Store(new { Name = "Test" }, "test/1");
                            session.SaveChanges();
                        }
                    });
                }
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void Windows_SsoUser_DomainMatchIsCaseInsensitive()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "bob";

            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, ssoUserId, SsoProvider.Windows, domain: "corp");

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite },
                    provider: SsoProvider.Windows, domain: "CORP");

                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCert,
                    Conventions = new DocumentConventions { DisposeCertificate = false, DisableTopologyUpdates = true }
                }.Initialize())
                {
                    using (var session = ssoStore.OpenSession())
                    {
                        session.Store(new { Name = "Test" }, "test/1");
                        session.SaveChanges();
                    }

                    using (var session = ssoStore.OpenSession())
                    {
                        var doc = session.Load<dynamic>("test/1");
                        Assert.NotNull(doc);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void SameIdentifier_DifferentProvider_IsRejected()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();
            var ssoUserId = "alice@example.com";

            // User entry is registered as Github; user cert is signed with the same SSO server but claims Google.
            var ssoUserCert = Certificates.CreateSsoUserCertificate(ssoCerts, ssoUserId, SsoProvider.Google);

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);
                Certificates.RegisterSsoUserEntry(certificates, ssoUserId, ssoCerts.SsoServerPublicKeyPinningHash,
                    new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite },
                    provider: SsoProvider.Github);

                using (var ssoStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = dbName,
                    Certificate = ssoUserCert,
                    Conventions = new DocumentConventions { DisposeCertificate = false, DisableTopologyUpdates = true }
                }.Initialize())
                {
                    Assert.Throws<AuthorizationException>(() =>
                    {
                        using (var session = ssoStore.OpenSession())
                        {
                            session.Store(new { Name = "Test" }, "test/1");
                            session.SaveChanges();
                        }
                    });
                }
            }
        }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        public void NonWindowsProvider_WithDomain_IsRejectedAtRegistration()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var ssoCerts = Certificates.GenerateAndSaveSsoTestCertificates();

            using (var adminStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                Certificates.RegisterSsoServerCert(certificates, ssoCerts);

                var ex = Assert.ThrowsAny<RavenException>(() =>
                {
                    Certificates.RegisterSsoUserEntry(certificates, "carol@example.com", ssoCerts.SsoServerPublicKeyPinningHash,
                        new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite },
                        provider: SsoProvider.Google, domain: "shouldNotBeAllowed");
                });

                Assert.Contains("Domain", ex.ToString(), StringComparison.OrdinalIgnoreCase);
            }
        }

        private static X509Certificate2 CreateSsoUserCertificateWithRawExtension(SsoTestCertificates ssoCerts, byte[] rawExtensionData)
        {
            const string ssoUserIdExtensionOid = "1.3.6.1.4.1.45751.2.2";

            using var userKey = RSA.Create(2048);
            var subjectName = new X500DistinguishedName("CN=SSO User Test");

            var request = new CertificateRequest(subjectName, userKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

            request.CertificateExtensions.Add(new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));

            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                new OidCollection { new Oid("1.3.6.1.5.5.7.3.2") }, false));

            request.CertificateExtensions.Add(new X509Extension(new Oid(ssoUserIdExtensionOid), rawExtensionData, false));

            byte[] serialNumber = new byte[20];
            using (var rng = RandomNumberGenerator.Create())
                rng.GetBytes(serialNumber);
            serialNumber[0] &= 0x7F;

            var signatureGenerator = X509SignatureGenerator.CreateForRSA(
                (RSA)ssoCerts.SsoServerPrivateKey, RSASignaturePadding.Pkcs1);

            var cert = request.Create(
                ssoCerts.SsoServerCert.SubjectName,
                signatureGenerator,
                DateTimeOffset.UtcNow.AddDays(-1),
                DateTimeOffset.UtcNow.AddYears(1),
                serialNumber);

            var certWithKey = cert.CopyWithPrivateKey(userKey);
            var pfxBytes = certWithKey.Export(X509ContentType.Pfx, string.Empty);
            return CertificateLoaderUtil.CreateCertificate(pfxBytes, flags: CertificateLoaderUtil.FlagsForPersist);
        }

        private static SsoTestCertificates CreateIndependentSsoServerCertificates()
        {
            var key = RSA.Create(2048);
            var req = new CertificateRequest("CN=Independent SSO Server", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment | X509KeyUsageFlags.KeyCertSign, false));
            req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                new OidCollection { new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2") }, false));
            var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
            return new SsoTestCertificates(
                cert,
                key,
                cert.GetPublicKeyPinningHash(),
                Convert.ToBase64String(cert.Export(X509ContentType.Cert)));
        }

        private static X509Certificate2 CreateRenewedSsoServerCert(SsoTestCertificates ssoCerts)
        {
            var ssoServerKey = (RSA)ssoCerts.SsoServerPrivateKey;
            var dn = new X500DistinguishedName("CN=Renewed SSO Server");
            var request = new CertificateRequest(dn, ssoServerKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment | X509KeyUsageFlags.KeyCertSign, false));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                new OidCollection
                {
                    new Oid("1.3.6.1.5.5.7.3.1"),
                    new Oid("1.3.6.1.5.5.7.3.2")
                }, false));

            var selfSigned = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-7), DateTimeOffset.UtcNow.AddMonths(3));
            var certBytes = selfSigned.Export(X509ContentType.Pfx, string.Empty);

#pragma warning disable SYSLIB0057
            return new X509Certificate2(certBytes, string.Empty,
                X509KeyStorageFlags.MachineKeySet | CertificateLoaderUtil.FlagsForExport);
#pragma warning restore SYSLIB0057
        }

    }
}

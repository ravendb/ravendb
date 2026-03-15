using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Authentication;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication
{
    public class AuthenticationBasicTests : DisableParallelTestBase
    {
        public AuthenticationBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        public X509Certificate2 CreateAndPutExpiredClientCertificate(string serverCertPath, X509Certificate2 serverCertificateForCommunication, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser)
        {
            var serverCertificateHolder = new SecretProtection(
                new SecurityConfiguration()).LoadCertificateFromPath(
                serverCertPath,
                null,
                Server.ServerStore.GetLicenseType(),
                Server.ServerStore.Configuration.Security.CertificateValidationKeyUsages);

            var clientCertificate = CertificateUtils.CreateSelfSignedExpiredClientCertificate("expired client cert", serverCertificateHolder.Certificate, serverCertificateHolder.PrivateKey);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = serverCertificateForCommunication,
                ClientCertificate = serverCertificateForCommunication
            }))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new PutClientCertificateOperation("expired client cert", clientCertificate, permissions, clearance)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                }
            }
            return clientCertificate;
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetDocWithValidPermission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.Single)]
        public void CanGetAttachmentWithValidPermission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Read
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                store.Operations.Send(new GetAttachmentOperation("test/1", "file.jpg", AttachmentType.Revision, "123"));
                store.Operations.Send(new GetAttachmentsOperation(new List<AttachmentRequest> { new("test/1", "file.jpg") }, AttachmentType.Document));
            }
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.Certificates)]
        [InlineData(null)] // framework default
        [InlineData("1.1")]
        [InlineData("2.0")]
        public void CanGetDocWithValidPermissionAndHttpVersion(string httpVersion)
        {
            var version = httpVersion != null ? new Version(httpVersion) : null;
            HttpVersionPolicy? versionPolicy = version == null || version.Major == 1 ? HttpVersionPolicy.RequestVersionOrLower : null;

            var customSettings = new Dictionary<string, string>();
            if (versionPolicy == HttpVersionPolicy.RequestVersionOrLower)
                customSettings.Add(RavenConfiguration.GetKey(x => x.Http.Protocols), HttpProtocols.Http1AndHttp2.ToString());

            var certificates = SetupServerAuthentication(Certificates, customSettings);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName,
                ModifyDocumentStore = s =>
                {
                    s.Conventions.HttpVersion = version;
                    s.Conventions.HttpVersionPolicy = versionPolicy;
                }
            }))
            {
                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CanReachOperatorEndpointWithOperatorPermission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                var doc = new DatabaseRecord($"WhateverDB-{Guid.NewGuid()}");
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc)); // operator operation
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CannotReachOperatorEndpointWithoutOperatorPermission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                var doc = new DatabaseRecord($"WhateverDB-{Guid.NewGuid()}");
                Assert.Throws<AuthorizationException>(() =>
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(doc)); // operator operation
                });
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CanReachDatabaseAdminEndpointWithDatabaseAdminPermission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                var ravenConnectionStr = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = dbName,
                };

                var result0 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr)); // DatabaseAdmin operation
                Assert.NotNull(result0.RaftCommandIndex);

                var result = store.Maintenance.Send(new GetConnectionStringsOperation(store.Database, ConnectionStringType.Raven));
                Assert.NotNull(result.RavenConnectionStrings);
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CannotReachDatabaseAdminEndpointWithoutDatabaseAdminPermission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                var ravenConnectionStr = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = dbName,
                };

                Assert.Throws<AuthorizationException>(() =>
                {
                    store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr)); // DatabaseAdmin operation
                });
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [InlineData(true)]
        [InlineData(false)]
        public void CanOnlyGetRelevantDbsAccordingToPermissions(bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var dbName1 = GetDatabaseName();
            var dbName2 = GetDatabaseName();

            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin,
                [dbName1] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
            using (GetDocumentStore(new Options // The databases are created inside GetDocumentStore
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName1
            }))
            using (GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName2
            }))
            {
                var names = store.Maintenance.Server.Send(new GetDatabaseNamesOperation(0, 25));
                Assert.True(names.Length == 2);
                Assert.True(names.Contains(dbName));
                Assert.True(names.Contains(dbName1));
                Assert.False(names.Contains(dbName2));
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public void CannotGetDocWithoutCertificate()
        {
            SetupServerAuthentication(Certificates);

            Assert.Throws<AuthorizationException>(() =>
            {
                // No certificate provided
                GetDocumentStore();
            });
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CannotGetDocWithInvalidPermission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var otherDbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [otherDbName] = DatabaseAccess.ReadWrite
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    StoreSampleDoc(store, "test/1");
                    using (var session = store.OpenSession())
                        session.Load<dynamic>("test/1");
                });
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public void CannotContactServerWhenNotUsingHttps()
        {
            var certificates = SetupServerAuthentication(Certificates, serverUrl: $"http://{Environment.MachineName}:0");
            Assert.Throws<InvalidOperationException>(() =>
            {
                Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            });
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [InlineData(true)]
        [InlineData(false)]
        public void CannotGetCertificateWithInvalidDbNamePermission(bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var e = Assert.Throws<RavenException>(() =>
            {
                Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>
                {
                    [dbName + "&*NOT__ALLOWED_NA$ %ME"] = DatabaseAccess.ReadWrite
                });
            });

            Assert.IsType<ArgumentException>(e.InnerException);
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CannotGetDocWithExpiredCertificate(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = CreateAndPutExpiredClientCertificate(certificates.ServerCertificatePath, certificates.ServerCertificateForCommunication.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    StoreSampleDoc(store, "test/1");
                });
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public void AllAdminRoutesHaveCorrectAuthorizationStatus()
        {
            var endpointsToIgnore = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "/admin/replication/conflicts/solver" // access handled internally
            };

            var routes = RouteScanner.Scan(attr =>
                endpointsToIgnore.Contains(attr.Path) == false && attr.Path.Contains("/admin/") && (attr.RequiredAuthorization != AuthorizationStatus.ClusterAdmin &&
                                                  attr.RequiredAuthorization != AuthorizationStatus.Operator &&
                                                  attr.RequiredAuthorization != AuthorizationStatus.DatabaseAdmin));
            Assert.Empty(routes);
        }

        [RavenFact(RavenTestCategory.Security)]
        public void AllAdminAuthorizationStatusHaveCorrectRoutes()
        {
            var routesToIgnore = new HashSet<string>
            {
                "/monitoring/snmp/oids",
                "/monitoring/snmp",
                "/monitoring/snmp/bulk",
                "/monitoring/snmp/mib"
            };

            var routes = RouteScanner.Scan(attr =>
                routesToIgnore.Contains(attr.Path) == false
                && !attr.Path.Contains("/admin/")
                && (attr.RequiredAuthorization == AuthorizationStatus.ClusterAdmin
                    || attr.RequiredAuthorization == AuthorizationStatus.Operator
                    || attr.RequiredAuthorization == AuthorizationStatus.DatabaseAdmin));

            Assert.Empty(routes);
        }

        [RavenFact(RavenTestCategory.Security)]
        public async Task EditClientCertificateOperation_WhenDo_ShouldEditCertificate()
        {
            const string certificateName = "Client&Certificate 2";

            var certificates = SetupServerAuthentication(Certificates);
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, certificateName: "ClientCertificate1");
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                ["SomeName"] = DatabaseAccess.ReadWrite
            }, certificateName: certificateName);

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var clientCertificate = certificates.ClientCertificate3.Value;
            await store.Maintenance.Server.SendAsync(new PutClientCertificateOperation(certificateName, clientCertificate, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin));

            var certsMetadata = await store.Maintenance.Server.SendAsync(new GetCertificatesMetadataOperation(certificateName));
            Assert.Equal(2, certsMetadata.Length);
            var certMetadata = certsMetadata.First();

            certMetadata.Permissions[store.Database] = DatabaseAccess.ReadWrite;
            var parameters = new EditClientCertificateOperation.Parameters
            {
                Thumbprint = certMetadata.Thumbprint,
                Name = certMetadata.Name,
                Permissions = certMetadata.Permissions,
                Clearance = certMetadata.SecurityClearance
            };
            await store.Maintenance.Server.SendAsync(new EditClientCertificateOperation(parameters));

            using (var testedStore = new DocumentStore
            {
                Database = store.Database,
                Urls = store.Urls,
                Certificate = clientCertificate,
                Conventions =
                {
                    DisposeCertificate = false
                }
            }.Initialize())
            {
                using var session = testedStore.OpenAsyncSession();
                await session.StoreAsync(new { Id = "someId" });
                await session.SaveChangesAsync();
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public async Task GetClientCertificateOperation_WhenNodeIsPassive_ShouldGetCertificate()
        {
            const string certificateName = "ClientCertificate2";

            var certificatesHolder = SetupServerAuthentication(Certificates);
            var certificates = new[]
            {
                (Name: certificateName, Certificate: certificatesHolder.ClientCertificate1.Value),
                (Name: certificateName, Certificate: certificatesHolder.ClientCertificate2.Value),
                (Name: "DifferentName", Certificate: certificatesHolder.ClientCertificate3.Value),
            };

            using var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                AdminCertificate = certificatesHolder.ServerCertificate.Value,
                ClientCertificate = certificatesHolder.ServerCertificate.Value,
            });

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var permissions = new Dictionary<string, DatabaseAccess>();
                const SecurityClearance clearance = SecurityClearance.ClusterAdmin;
                foreach (var (name, certificate) in certificates)
                {
                    var certBytes = certificate.Export(X509ContentType.Cert);
                    var certDef = new CertificateDefinition { Name = name, Permissions = permissions, SecurityClearance = clearance };
                    await AdminCertificatesHandler.PutCertificateCollectionInCluster(certDef, certBytes, string.Empty, Server.ServerStore, ctx, null, RaftIdGenerator.NewId());
                }
            }
            {
                var certsMetadata = await store.Maintenance.Server.SendAsync(new GetCertificatesMetadataOperation());
                Assert.Equal(4, certsMetadata.Length);

                var certsMetadataByName = await store.Maintenance.Server.SendAsync(new GetCertificatesMetadataOperation(certificateName));
                Assert.Equal(2, certsMetadataByName.Length);

                var certMetadata = await store.Maintenance.Server.SendAsync(new GetCertificateMetadataOperation(certificates[0].Certificate.Thumbprint));
                Assert.NotNull(certMetadata);
            }

            await Server.ServerStore.EnsureNotPassiveAsync();
            {
                var certsMetadata = await store.Maintenance.Server.SendAsync(new GetCertificatesMetadataOperation());
                Assert.Equal(4, certsMetadata.Length);

                var certsMetadataByName = await store.Maintenance.Server.SendAsync(new GetCertificatesMetadataOperation(certificateName));
                Assert.Equal(2, certsMetadataByName.Length);

                var certMetadata = await store.Maintenance.Server.SendAsync(new GetCertificateMetadataOperation(certificates[0].Certificate.Thumbprint));
                Assert.NotNull(certMetadata);
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public async Task GetCertificate_WhenMetadataOnly_ShouldNotSendTheCertificateItself()
        {
            const string certificateName = "ClientCertificate";

            var certificates = SetupServerAuthentication(Certificates);
            var serverCert = certificates.ServerCertificateForCommunication.Value;
            var permissions = new Dictionary<string, DatabaseAccess>();

            var adminCert = Certificates.RegisterClientCertificate(serverCert, certificates.ClientCertificate1.Value, permissions, SecurityClearance.ClusterAdmin, certificateName: certificateName);
            Certificates.RegisterClientCertificate(serverCert, certificates.ClientCertificate2.Value, permissions, certificateName: certificateName);

            using var store = GetDocumentStore(new Options { AdminCertificate = adminCert, ClientCertificate = adminCert, });

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var operation = new GetCertificateMetadataOperation(adminCert.Thumbprint);
                var json = await ExecuteOperation(operation, context);
                var results = JsonDeserializationClient.GetCertificatesResponse(json).Results;

                Assert.All(results, c => Assert.Null(c.Certificate));
            }

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var operation = new GetCertificatesMetadataOperation(certificateName);
                var json = await ExecuteOperation(operation, context);
                var results = JsonDeserializationClient.GetCertificatesResponse(json).Results;
                RavenTestHelper.AssertAll(
                    () => Assert.All(results, c => Assert.Null(c.Certificate)),
                    () => Assert.All(results, c => Assert.Equal(certificateName, c.Name)),
                    () => Assert.Equal(2, results.Length));
            }

            async Task<BlittableJsonReaderObject> ExecuteOperation<T>(IServerOperation<T> operation, JsonOperationContext context)
            {
                var command = operation.GetCommand(store.Conventions, context);
                var request = command.CreateRequest(context, new ServerNode { Url = store.Urls.First() }, out var url).WithConventions(store.Conventions);
                request.RequestUri = new Uri(url);
                var client = store.GetRequestExecutor(store.Database).HttpClient;
                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
                response.EnsureSuccessStatusCode();
                var content = await response.Content.ReadAsStreamAsync();
                return await context.ReadForMemoryAsync(content, "response/object");
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetDocWith_Read_Permission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Read
            });

            var clone = options.Clone();
            clone.AdminCertificate = adminCert;
            clone.ClientCertificate = adminCert;
            clone.ModifyDatabaseName = _ => dbName;
            clone.DeleteDatabaseOnDispose = false;

            using (var store = GetDocumentStore(clone))
            {
                StoreSampleDoc(store, "test/1");
            }

            clone = options.Clone();
            clone.AdminCertificate = adminCert;
            clone.ClientCertificate = userCert;
            clone.ModifyDatabaseName = _ => dbName;
            clone.CreateDatabase = false;

            using (var store = GetDocumentStore(clone))
            {
                using (var session = store.OpenSession())
                {
                    var test1Doc = session.Load<dynamic>("test/1");

                    Assert.NotNull(test1Doc);
                }

                using (var session = store.OpenSession())
                {
                    var test1Doc = session.Advanced.Lazily.Load<dynamic>("test/1").Value; // multi-get

                    Assert.NotNull(test1Doc);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CannotPutDocWith_Read_Permission_MultiGet(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Read
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    var command = new MultiGetCommand(commands.RequestExecutor, new List<GetRequest>
                    {
                        new GetRequest
                        {
                            Url = "/docs",
                            Method = HttpMethod.Get,
                            Query = "?id=samples/1"
                        },
                        new GetRequest
                        {
                            Url = "/admin/configuration/settings",
                            Method = HttpMethod.Get
                        }
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = command.Result;
                    Assert.Equal(2, results.Count);
                    Assert.Equal(HttpStatusCode.NotFound, results[0].StatusCode);
                    Assert.Equal(HttpStatusCode.Forbidden, results[1].StatusCode);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CannotPutDocWith_Read_Permission(Options options, bool with2Eku)
        {
            var certificates = SetupServerAuthentication(Certificates, with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Read
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = _ => dbName;

            using (var store = GetDocumentStore(options))
            {
                Assert.Throws<AuthorizationException>(() => StoreSampleDoc(store, "test/1"));
            }
        }

        internal static TestCertificatesHolder SetupServerAuthentication(CertificatesTestBase certificatesBase, Dictionary<string, string> customSettings = null, string serverUrl = null, TestCertificatesHolder certificates = null, bool with2Eku = true)
        {
            customSettings ??= new Dictionary<string, string>();

            customSettings[RavenConfiguration.GetKey(x => x.Licensing.CanActivate)] = "false";
            customSettings[RavenConfiguration.GetKey(x => x.Licensing.CanForceUpdate)] = "false";
            customSettings[RavenConfiguration.GetKey(x => x.Licensing.CanRenew)] = "false";

            return certificatesBase.SetupServerAuthentication(customSettings, serverUrl, certificates, with2Eku: with2Eku);
        }

        private static void StoreSampleDoc(DocumentStore store, string docName)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new
                {
                    Name = "test auth"
                },
                docName);
                session.SaveChanges();
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public async Task DisabledCertificate_ShouldBeRejected()
        {
            var certificates = SetupServerAuthentication(Certificates);
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, certificateName: "AdminCert");

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            // Register user cert with permission to the actual database
            Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [store.Database] = DatabaseAccess.ReadWrite
            }, certificateName: "UserCert");

            // Disable the user certificate
            var parameters = new EditClientCertificateOperation.Parameters
            {
                Thumbprint = certificates.ClientCertificate2.Value.Thumbprint,
                Name = "UserCert",
                Permissions = new Dictionary<string, DatabaseAccess> { [store.Database] = DatabaseAccess.ReadWrite },
                Clearance = SecurityClearance.ValidUser,
                Disabled = true
            };
            await store.Maintenance.Server.SendAsync(new EditClientCertificateOperation(parameters));

            // Verify the disabled certificate is rejected
            using (var disabledStore = new DocumentStore
            {
                Database = store.Database,
                Urls = store.Urls,
                Certificate = certificates.ClientCertificate2.Value,
                Conventions = { DisposeCertificate = false }
            }.Initialize())
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    using var session = disabledStore.OpenSession();
                    session.Load<dynamic>("test/1");
                });
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public async Task DisabledCertificate_CanBeReEnabled()
        {
            var certificates = SetupServerAuthentication(Certificates);
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, certificateName: "AdminCert");

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            // Register user cert with permission to the actual database
            Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [store.Database] = DatabaseAccess.ReadWrite
            }, certificateName: "UserCert");

            var permissions = new Dictionary<string, DatabaseAccess> { [store.Database] = DatabaseAccess.ReadWrite };

            // Disable the user certificate
            var parameters = new EditClientCertificateOperation.Parameters
            {
                Thumbprint = certificates.ClientCertificate2.Value.Thumbprint,
                Name = "UserCert",
                Permissions = permissions,
                Clearance = SecurityClearance.ValidUser,
                Disabled = true
            };
            await store.Maintenance.Server.SendAsync(new EditClientCertificateOperation(parameters));

            // Re-enable the certificate
            parameters.Disabled = false;
            await store.Maintenance.Server.SendAsync(new EditClientCertificateOperation(parameters));

            // Verify the re-enabled certificate works
            using (var reEnabledStore = new DocumentStore
            {
                Database = store.Database,
                Urls = store.Urls,
                Certificate = certificates.ClientCertificate2.Value,
                Conventions = { DisposeCertificate = false }
            }.Initialize())
            {
                using var session = reEnabledStore.OpenAsyncSession();
                await session.StoreAsync(new { Id = "test/1" });
                await session.SaveChangesAsync();
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public async Task CannotDisableClusterNodeCertificate()
        {
            var certificates = SetupServerAuthentication(Certificates);
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, certificateName: "AdminCert");

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            // Register a ClusterNode certificate
            await store.Maintenance.Server.SendAsync(new PutClientCertificateOperation("NodeCert", certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterNode));

            // Try to disable it — should fail
            var parameters = new EditClientCertificateOperation.Parameters
            {
                Thumbprint = certificates.ClientCertificate2.Value.Thumbprint,
                Name = "NodeCert",
                Permissions = new Dictionary<string, DatabaseAccess>(),
                Clearance = SecurityClearance.ClusterNode,
                Disabled = true
            };
            await Assert.ThrowsAsync<RavenException>(async () =>
            {
                await store.Maintenance.Server.SendAsync(new EditClientCertificateOperation(parameters));
            });
        }

        [RavenFact(RavenTestCategory.Security)]
        public async Task CannotDisableOwnCertificate()
        {
            var certificates = SetupServerAuthentication(Certificates);
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, certificateName: "AdminCert");

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            // Try to disable the admin certificate currently in use
            var parameters = new EditClientCertificateOperation.Parameters
            {
                Thumbprint = certificates.ClientCertificate1.Value.Thumbprint,
                Name = "AdminCert",
                Permissions = new Dictionary<string, DatabaseAccess>(),
                Clearance = SecurityClearance.ClusterAdmin,
                Disabled = true
            };
            await Assert.ThrowsAsync<RavenException>(async () =>
            {
                await store.Maintenance.Server.SendAsync(new EditClientCertificateOperation(parameters));
            });
        }
    }
}


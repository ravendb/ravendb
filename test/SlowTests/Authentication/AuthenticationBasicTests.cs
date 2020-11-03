// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
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
using Raven.Server.Config.Categories;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Authentication;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Authentication
{
    public class AuthenticationBasicTests : RavenTestBase
    {
        public AuthenticationBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        public X509Certificate2 CreateAndPutExpiredClientCertificate(string serverCertPath, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser)
        {
            var serverCertificate = new X509Certificate2(serverCertPath, (string)null, X509KeyStorageFlags.MachineKeySet);
            var serverCertificateHolder = new SecretProtection(new SecurityConfiguration()).LoadCertificateFromPath(serverCertPath, null, Server.ServerStore);

            var clientCertificate = CertificateUtils.CreateSelfSignedExpiredClientCertificate("expired client cert", serverCertificateHolder);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = serverCertificate,
                ClientCertificate = serverCertificate
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

        [Fact]
        public void CanGetDocWithValidPermission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
            {
                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
            }
        }

        [Theory]
        [InlineData(null)] // framework default
        [InlineData("1.1")]
        [InlineData("2.0")]
        public void CanGetDocWithValidPermissionAndHttpVersion(string httpVersion)
        {
            var version = httpVersion != null ? new Version(httpVersion) : null;

            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName,
                ModifyDocumentStore = s => s.Conventions.HttpVersion = version
            }))
            {
                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
            }
        }

        [Fact]
        public void CanReachOperatorEndpointWithOperatorPermission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
            {
                var doc = new DatabaseRecord($"WhateverDB-{Guid.NewGuid()}");
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc)); // operator operation
            }
        }

        [Fact]
        public void CannotReachOperatorEndpointWithoutOperatorPermission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
            {
                var doc = new DatabaseRecord($"WhateverDB-{Guid.NewGuid()}");
                Assert.Throws<AuthorizationException>(() =>
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(doc)); // operator operation
                });
            }
        }

        [Fact]
        public void CanReachDatabaseAdminEndpointWithDatabaseAdminPermission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
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

        [Fact]
        public void CannotReachDatabaseAdminEndpointWithoutDatabaseAdminPermission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
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

        [Fact]
        public void CanOnlyGetRelevantDbsAccordingToPermissions()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var dbName1 = GetDatabaseName();
            var dbName2 = GetDatabaseName();

            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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

        [Fact]
        public void CannotGetDocWithoutCertificate()
        {
            SetupServerAuthentication();

            Assert.Throws<AuthorizationException>(() =>
            {
                // No certificate provided
                GetDocumentStore();
            });
        }

        [Fact]
        public void CannotGetDocWithInvalidPermission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var otherDbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [otherDbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    StoreSampleDoc(store, "test/1");
                    using (var session = store.OpenSession())
                        session.Load<dynamic>("test/1");
                });
            }
        }

        [Fact]
        public void CannotContactServerWhenNotUsingHttps()
        {
            var certificates = SetupServerAuthentication(serverUrl: $"http://{Environment.MachineName}:0");
            Assert.Throws<InvalidOperationException>(() =>
            {
                RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            });
        }

        [Fact]
        public void CannotGetCertificateWithInvalidDbNamePermission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var e = Assert.Throws<RavenException>(() =>
            {
                RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>
                {
                    [dbName + "&*NOT__ALLOWED_NA$ %ME"] = DatabaseAccess.ReadWrite
                });
            });

            Assert.IsType<ArgumentException>(e.InnerException);
        }

        [Fact]
        public void CannotGetDocWithExpiredCertificate()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = CreateAndPutExpiredClientCertificate(certificates.ServerCertificatePath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
            {
                Assert.Throws<AuthorizationException>(() =>
                {
                    StoreSampleDoc(store, "test/1");
                });
            }
        }

        [Fact]
        public void AllAdminRoutesHaveCorrectAuthorizationStatus()
        {
            var routes = RouteScanner.Scan(attr =>
                attr.Path.Contains("/admin/") && (attr.RequiredAuthorization != AuthorizationStatus.ClusterAdmin &&
                                                  attr.RequiredAuthorization != AuthorizationStatus.Operator &&
                                                  attr.RequiredAuthorization != AuthorizationStatus.DatabaseAdmin));
            Assert.Empty(routes);
        }

        [Fact]
        public void AllAdminAuthorizationStatusHaveCorrectRoutes()
        {
            var routesToIgnore = new HashSet<string>
            {
                "/monitoring/snmp/oids",
                "/monitoring/snmp",
                "/monitoring/snmp/bulk"
            };

            var routes = RouteScanner.Scan(attr =>
                routesToIgnore.Contains(attr.Path) == false
                && !attr.Path.Contains("/admin/")
                && (attr.RequiredAuthorization == AuthorizationStatus.ClusterAdmin
                    || attr.RequiredAuthorization == AuthorizationStatus.Operator
                    || attr.RequiredAuthorization == AuthorizationStatus.DatabaseAdmin));

            Assert.Empty(routes);
        }

        [Fact]
        public async Task EditClientCertificateOperation_WhenDo_ShouldEditCertificate()
        {
            const string certificateName = "Client&Certificate 2";

            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, certificateName: "ClientCertificate1");
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
                Certificate = clientCertificate
            }.Initialize())
            {
                using var session = testedStore.OpenAsyncSession();
                await session.StoreAsync(new { Id = "someId" });
                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task GetClientCertificateOperation_WhenNodeIsPassive_ShouldGetCertificate()
        {
            const string certificateName = "ClientCertificate2";

            var certificatesHolder = SetupServerAuthentication();
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
                    await AdminCertificatesHandler.PutCertificateCollectionInCluster(certDef, certBytes, string.Empty, Server.ServerStore, ctx, RaftIdGenerator.NewId());
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

        [Fact]
        public async Task GetCertificate_WhenMetadataOnly_ShouldNotSendTheCertificateItself()
        {
            const string certificateName = "ClientCertificate";

            var certificates = SetupServerAuthentication();
            var serverCert = certificates.ServerCertificate.Value;
            var permissions = new Dictionary<string, DatabaseAccess>();

            var adminCert = RegisterClientCertificate(serverCert, certificates.ClientCertificate1.Value, permissions, SecurityClearance.ClusterAdmin, certificateName: certificateName);
            RegisterClientCertificate(serverCert, certificates.ClientCertificate2.Value, permissions, certificateName: certificateName);

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
                var request = command.CreateRequest(context, new ServerNode { Url = store.Urls.First() }, out var url);
                request.RequestUri = new Uri(url);
                var client = store.GetRequestExecutor(store.Database).HttpClient;
                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
                response.EnsureSuccessStatusCode();
                var content = await response.Content.ReadAsStreamAsync();
                return await context.ReadForMemoryAsync(content, "response/object").ConfigureAwait(false);
            }
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
    }
}

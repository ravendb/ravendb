// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.MultiGet;
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
using Tests.Infrastructure;
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

        [Fact]
        public void CanGetDocWith_Read_Permission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Read
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                DeleteDatabaseOnDispose = false
            }))
            {
                StoreSampleDoc(store, "test/1");
            }

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName,
                CreateDatabase = false
            }))
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

        [Fact]
        public void CannotPutDocWith_Read_Permission_MultiGet()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Read
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName
            }))
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

        [Fact]
        public void CannotPutDocWith_Read_Permission()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Read
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => dbName,
                DeleteDatabaseOnDispose = false
            }))
            {
                Assert.Throws<AuthorizationException>(() => StoreSampleDoc(store, "test/1"));
            }
        }

        [Fact]
        public void Routes_Conventions()
        {
            foreach (var route in RouteScanner.AllRoutes.Values)
            {
                if (IsDatabaseRoute(route))
                {
                    AssertDatabaseRoute(route);
                    return;
                }

                AssertServerRoute(route);
            }

            static bool IsDatabaseRoute(RouteInformation route)
            {
                return route.Path.Contains("/databases/*/", StringComparison.OrdinalIgnoreCase);
            }

            static void AssertDatabaseRoute(RouteInformation route)
            {
                if (string.Equals(route.Method, "OPTIONS", StringComparison.OrdinalIgnoreCase) == false) // artificially added routes for CORS
                    Assert.True(RouteInformation.RouteType.Databases == route.TypeOfRoute, $"{route.Method} {route.Path} - {route.AuthorizationStatus}");

                Assert.True(route.AuthorizationStatus == AuthorizationStatus.ValidUser
                    || route.AuthorizationStatus == AuthorizationStatus.DatabaseAdmin, $"{route.Method} {route.Path} - {route.AuthorizationStatus}");
            }

            static void AssertServerRoute(RouteInformation route)
            {
                Assert.True(route.AuthorizationStatus == AuthorizationStatus.ValidUser
                    || route.AuthorizationStatus == AuthorizationStatus.ClusterAdmin
                    || route.AuthorizationStatus == AuthorizationStatus.Operator
                    || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess
                    || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients, $"{route.Method} {route.Path} - {route.AuthorizationStatus}");
            }
        }

        [Fact]
        public void Routes_Database_Read()
        {
            var certificates = SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [databaseName1] = DatabaseAccess.Read
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;
                        if (route.EndpointType == EndpointType.Write)
                            canAccess = false;
                        else
                        {
                            canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser
                            || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients
                            || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess;
                        }

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;
                        if (route.EndpointType == EndpointType.Write)
                            canAccess = false;
                        else
                        {
                            canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser;
                        }

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = false;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        [Fact]
        public void Routes_Database_ReadWrite()
        {
            var certificates = SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [databaseName1] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;
                        if (route.EndpointType == EndpointType.Write)
                            canAccess = false;
                        else
                        {
                            canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser
                            || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients
                            || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess;
                        }

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = false;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        [Fact]
        public void Routes_Database_Admin()
        {
            var certificates = SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [databaseName1] = DatabaseAccess.Admin
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;
                        if (route.EndpointType == EndpointType.Write)
                            canAccess = false;
                        else
                        {
                            canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser
                            || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients
                            || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess;
                        }

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = false;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        [NightlyBuildFact64Bit]
        public void Routes_Operator()
        {
            var certificates = SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = route.AuthorizationStatus == AuthorizationStatus.Operator
                            || route.AuthorizationStatus == AuthorizationStatus.ValidUser
                            || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients
                            || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        [NightlyBuildFact64Bit]
        public void Routes_ClusterAdmin()
        {
            var certificates = SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { Server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        private void AssertServerRoutes(IEnumerable<RouteInformation> routes, HashSet<(string Method, string Path)> endpointsToIgnore, HttpClient httpClient, Action<RouteInformation, HttpStatusCode> assert)
        {
            foreach (var route in routes)
            {
                if (route.TypeOfRoute != RouteInformation.RouteType.None)
                    continue;

                if (route.Method == "OPTIONS")
                    continue; // artificially added routes for CORS

                if (endpointsToIgnore.Contains((route.Method, route.Path)))
                    continue;

                var response = httpClient.Send(new HttpRequestMessage
                {
                    Method = new HttpMethod(route.Method),
                    RequestUri = new Uri(route.Path, UriKind.Relative)
                });

                assert(route, response.StatusCode);
            }
        }

        private void AssertDatabaseRoutes(IEnumerable<RouteInformation> routes, string databaseName, HttpClient httpClient, Action<RouteInformation, HttpStatusCode> assert)
        {
            foreach (var route in routes)
            {
                if (route.TypeOfRoute != RouteInformation.RouteType.Databases)
                    continue;

                if (route.Method == "OPTIONS")
                    continue; // artificially added routes for CORS

                var response = httpClient.Send(new HttpRequestMessage
                {
                    Method = new HttpMethod(route.Method),
                    RequestUri = new Uri(route.Path.Replace("/databases/*/", $"/databases/{databaseName}/", StringComparison.OrdinalIgnoreCase), UriKind.Relative)
                });

                assert(route, response.StatusCode);
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

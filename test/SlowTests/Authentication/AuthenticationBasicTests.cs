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
            var serverCertificateHolder = new SecretProtection(
                new SecurityConfiguration()).LoadCertificateFromPath(
                serverCertPath,
                null,
                Server.ServerStore.GetLicenseType(),
                Server.ServerStore.Configuration.Security.CertificateValidationKeyUsages);

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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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

        [Fact]
        public void CanGetAttachmentWithValidPermission()
        {
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
                store.Operations.Send(new GetAttachmentOperation("test/1", "file.jpg", AttachmentType.Revision, "123"));
                store.Operations.Send(new GetAttachmentsOperation(new List<AttachmentRequest> { new("test/1", "file.jpg") }, AttachmentType.Document));
            }
        }

        [Theory]
        [InlineData(null)] // framework default
        [InlineData("1.1")]
        [InlineData("2.0")]
        public void CanGetDocWithValidPermissionAndHttpVersion(string httpVersion)
        {
            var version = httpVersion != null ? new Version(httpVersion) : null;

            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var dbName1 = GetDatabaseName();
            var dbName2 = GetDatabaseName();

            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
            SetupServerAuthentication(Certificates);

            Assert.Throws<AuthorizationException>(() =>
            {
                // No certificate provided
                GetDocumentStore();
            });
        }

        [Fact]
        public void CannotGetDocWithInvalidPermission()
        {
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var otherDbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
            var certificates = SetupServerAuthentication(Certificates, serverUrl: $"http://{Environment.MachineName}:0");
            Assert.Throws<InvalidOperationException>(() =>
            {
                Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            });
        }

        [Fact]
        public void CannotGetCertificateWithInvalidDbNamePermission()
        {
            var certificates = SetupServerAuthentication(Certificates);
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

        [Fact]
        public void CannotGetDocWithExpiredCertificate()
        {
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
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

            var certificates = SetupServerAuthentication(Certificates);
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, certificateName: "ClientCertificate1");
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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

        [Fact]
        public async Task GetCertificate_WhenMetadataOnly_ShouldNotSendTheCertificateItself()
        {
            const string certificateName = "ClientCertificate";

            var certificates = SetupServerAuthentication(Certificates);
            var serverCert = certificates.ServerCertificate.Value;
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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
            var certificates = SetupServerAuthentication(Certificates);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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

        internal static TestCertificatesHolder SetupServerAuthentication(CertificatesTestBase certificatesBase, Dictionary<string, string> customSettings = null, string serverUrl = null, TestCertificatesHolder certificates = null)
        {
            customSettings ??= new Dictionary<string, string>();

            customSettings[RavenConfiguration.GetKey(x => x.Licensing.CanActivate)] = "false";
            customSettings[RavenConfiguration.GetKey(x => x.Licensing.CanForceUpdate)] = "false";
            customSettings[RavenConfiguration.GetKey(x => x.Licensing.CanRenew)] = "false";

            return certificatesBase.SetupServerAuthentication(customSettings, serverUrl, certificates);
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

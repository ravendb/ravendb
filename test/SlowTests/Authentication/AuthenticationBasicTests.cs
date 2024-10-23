﻿// -----------------------------------------------------------------------
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
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
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
            var serverCertificate = CertificateHelper.CreateCertificateFromPfx(serverCertPath, (string)null, X509KeyStorageFlags.UserKeySet);
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

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetDocWithValidPermission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanGetAttachmentWithValidPermission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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

        [Theory]
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

            var certificates = Certificates.SetupServerAuthentication(customSettings);
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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanReachOperatorEndpointWithOperatorPermission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CannotReachOperatorEndpointWithoutOperatorPermission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanReachDatabaseAdminEndpointWithDatabaseAdminPermission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CannotReachDatabaseAdminEndpointWithoutDatabaseAdminPermission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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

        [Fact]
        public void CanOnlyGetRelevantDbsAccordingToPermissions()
        {
            var certificates = Certificates.SetupServerAuthentication();
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
            Certificates.SetupServerAuthentication();

            Assert.Throws<AuthorizationException>(() =>
            {
                // No certificate provided
                GetDocumentStore();
            });
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CannotGetDocWithInvalidPermission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var otherDbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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

        [Fact]
        public void CannotContactServerWhenNotUsingHttps()
        {
            var certificates = Certificates.SetupServerAuthentication(serverUrl: $"http://{Environment.MachineName}:0");
            Assert.Throws<InvalidOperationException>(() =>
            {
                Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            });
        }

        [Fact]
        public void CannotGetCertificateWithInvalidDbNamePermission()
        {
            var certificates = Certificates.SetupServerAuthentication();
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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CannotGetDocWithExpiredCertificate(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = CreateAndPutExpiredClientCertificate(certificates.ServerCertificatePath, new Dictionary<string, DatabaseAccess>
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

        [Fact]
        public async Task EditClientCertificateOperation_WhenDo_ShouldEditCertificate()
        {
            const string certificateName = "Client&Certificate 2";

            var certificates = Certificates.SetupServerAuthentication();
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

        [Fact]
        public async Task GetClientCertificateOperation_WhenNodeIsPassive_ShouldGetCertificate()
        {
            const string certificateName = "ClientCertificate2";

            var certificatesHolder = Certificates.SetupServerAuthentication();
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

            var certificates = Certificates.SetupServerAuthentication();
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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetDocWith_Read_Permission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CannotPutDocWith_Read_Permission_MultiGet(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CannotPutDocWith_Read_Permission(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
        public async Task Routes_Database_Read()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
                    Certificate = adminCert,
                    Conventions =
                    {
                        DisposeCertificate = false
                    }
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
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler).WithConventions(DocumentConventions.DefaultForServer);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    await AssertServerRoutesAsync(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
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

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
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

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
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
        public async Task Routes_Database_ReadWrite()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
                    Certificate = adminCert,
                    Conventions =
                    {
                        DisposeCertificate = false
                    }
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
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler).WithConventions(DocumentConventions.DefaultForServer);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    await AssertServerRoutesAsync(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
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

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
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
        public async Task Routes_Database_Admin()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
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
                    Certificate = adminCert,
                    Conventions =
                    {
                        DisposeCertificate = false
                    }
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
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler).WithConventions(DocumentConventions.DefaultForServer);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    await AssertServerRoutesAsync(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
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

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
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

        [NightlyBuildMultiplatformFact(RavenArchitecture.AllX64)]
        public async Task Routes_Operator()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

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
                    Certificate = adminCert,
                    Conventions =
                    {
                        DisposeCertificate = false
                    }
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
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler).WithConventions(DocumentConventions.DefaultForServer);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    await AssertServerRoutesAsync(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
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

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
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

        [RavenMultiplatformFact(RavenTestCategory.Certificates, RavenArchitecture.AllX64, NightlyBuildOnly = true)]
        public async Task Routes_ClusterAdmin()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

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
                    Certificate = adminCert,
                    Conventions =
                    {
                        DisposeCertificate = false
                    }
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
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler).WithConventions(DocumentConventions.DefaultForServer);
                    httpClient.BaseAddress = new Uri(Server.WebUrl);

                    await AssertServerRoutesAsync(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    await AssertDatabaseRoutesAsync(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
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

        private static async Task AssertServerRoutesAsync(IEnumerable<RouteInformation> routes, HashSet<(string Method, string Path)> endpointsToIgnore, HttpClient httpClient, Action<RouteInformation, HttpStatusCode> assert)
        {
            foreach (var route in routes)
            {
                if (route.TypeOfRoute != RouteInformation.RouteType.None)
                    continue;

                if (route.Method == "OPTIONS")
                    continue; // artificially added routes for CORS

                if (endpointsToIgnore.Contains((route.Method, route.Path)))
                    continue;

                var requestUri = new Uri(route.Path, UriKind.Relative);
                HttpResponseMessage response;
                try
                {
                    response = await httpClient.SendAsync(new HttpRequestMessage
                    {
                        Method = new HttpMethod(route.Method),
                        RequestUri = new Uri(route.Path, UriKind.Relative)
                    }.WithConventions(DocumentConventions.DefaultForServer));
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Could not get response from {route.Method} '{requestUri}'.", e);
                }

                assert(route, response.StatusCode);
            }
        }

        private static async Task AssertDatabaseRoutesAsync(IEnumerable<RouteInformation> routes, HashSet<(string Method, string Path)> endpointsToIgnore, string databaseName, HttpClient httpClient, Action<RouteInformation, HttpStatusCode> assert)
        {
            foreach (var route in routes)
            {
                if (route.TypeOfRoute != RouteInformation.RouteType.Databases)
                    continue;

                if (route.Method == "OPTIONS")
                    continue; // artificially added routes for CORS

                if (endpointsToIgnore.Contains((route.Method, route.Path)))
                    continue;

                var requestUri = new Uri(route.Path.Replace("/databases/*/", $"/databases/{databaseName}/", StringComparison.OrdinalIgnoreCase), UriKind.Relative);
                HttpResponseMessage response;
                try
                {
                    response = await httpClient.SendAsync(new HttpRequestMessage
                    {
                        Method = new HttpMethod(route.Method),
                        RequestUri = requestUri
                    }.WithConventions(DocumentConventions.DefaultForServer));
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Could not get response from {route.Method} '{requestUri}'.", e);
                }

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

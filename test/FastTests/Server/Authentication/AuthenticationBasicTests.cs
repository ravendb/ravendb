// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Authentication
{
    public class AuthenticationBasicTests : RavenTestBase
    {
        public X509Certificate2 CreateAndPutExpiredClientCertificate(string serverCertPath, Dictionary<string, DatabaseAccess> permissions, bool serverAdmin = false)
        {
            var serverCertificate = new X509Certificate2(serverCertPath);
            var serverCertificateHolder = new SecretProtection(new SecurityConfiguration()).LoadCertificateFromPath(serverCertPath, null);

            var clientCertificate = CertificateUtils.CreateSelfSignedExpiredClientCertificate("expired client cert", serverCertificateHolder);

            using (var store = GetDocumentStore(adminCertificate: serverCertificate, userCertificate: serverCertificate))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new PutClientCertificateOperation(clientCertificate, permissions, serverAdmin)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                }
            }
            return clientCertificate;
        }

        [Fact]
        public void CanGetDocWithValidPermission()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            var userCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(adminCertificate: adminCert, userCertificate:userCert, modifyName: s => dbName))
            {
                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
            }
        }

        [Fact]
        public void CanReachServerAdminEndpointWithServerAdminPermission()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            var userCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            
            using (var store = GetDocumentStore(adminCertificate: adminCert, userCertificate:userCert, modifyName:(s => dbName)))
            {
                var doc = new DatabaseRecord("WhateverDB");
                store.Admin.Server.Send(new CreateDatabaseOperation(doc)); // ServerAdmin operation
            }
        }

        [Fact]
        public void CannotReachServerAdminEndpointWithoutServerAdminPermission()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            var userCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(adminCertificate: adminCert, userCertificate: userCert, modifyName: (s => dbName)))
            {
                var doc = new DatabaseRecord("WhateverDB");
                Assert.Throws<AuthorizationException>(() =>
                {
                    store.Admin.Server.Send(new CreateDatabaseOperation(doc)); // ServerAdmin operation
                });
            }
        }
        
        [Fact]
        public void CanReachDatabaseAdminEndpointWithDatabaseAdminPermission()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            var userCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });

            using (var store = GetDocumentStore(adminCertificate: adminCert, userCertificate: userCert, modifyName: (s => dbName)))
            {
                var ravenConnectionStr = new RavenConnectionString()
                {
                    Name = $"RavenConnectionString",
                    Url = $"http://127.0.0.1:8080",
                    Database = dbName,
                };

                store.Admin.Server.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr, store.Database)); // DatabaseAdmin operation
                var result = store.Admin.Server.Send(new GetConnectionStringsOperation(store.Database));
                Assert.NotNull(result.RavenConnectionStrings);
            }
        }

        [Fact]
        public void CannotReachDatabaseAdminEndpointWithoutDatabaseAdminPermission()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            var userCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(adminCertificate: adminCert, userCertificate: userCert, modifyName: (s => dbName)))
            {
                var ravenConnectionStr = new RavenConnectionString()
                {
                    Name = $"RavenConnectionString",
                    Url = $"http://127.0.0.1:8080",
                    Database = dbName,
                };

                Assert.Throws<AuthorizationException>(() =>
                {
                    store.Admin.Server.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr, store.Database)); // DatabaseAdmin operation
                });
            }
        }

        [Fact]
        public void CanOnlyGetRelevantDbsAccordingToPermissions()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var dbName1 = GetDatabaseName();
            var dbName2 = GetDatabaseName();

            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            var userCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin,
                [dbName1] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(adminCertificate: adminCert, userCertificate: userCert, modifyName: (s => dbName)))
            using (GetDocumentStore(adminCertificate: adminCert, userCertificate: userCert, modifyName: (s => dbName1))) // The databases are created inside GetDocumentStore
            using (GetDocumentStore(adminCertificate: adminCert, userCertificate: userCert, modifyName: (s => dbName2)))
            {
                var names = store.Admin.Server.Send(new GetDatabaseNamesOperation(0, 25));
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
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var otherDbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            var userCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [otherDbName] = DatabaseAccess.ReadWrite
            });
            
            using (var store = GetDocumentStore(adminCertificate: adminCert, userCertificate: userCert, modifyName: (s => dbName)))
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
            var serverCertPath = SetupServerAuthentication(serverUrl: "http://" + Environment.MachineName + ":8080");
            Assert.Throws<System.InvalidOperationException>(() =>
            {
                AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            });
        }

        [Fact]
        public void CannotGetCertificateWithInvalidDbNamePermission()
        {
            
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var e = Assert.Throws<RavenException>(() =>
            {
                AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
                {
                    [dbName + "&*NOT__ALLOWED_NA$ %ME"] = DatabaseAccess.ReadWrite
                });
            });
            
            Assert.IsType<InvalidOperationException>(e.InnerException);
        }

        [Fact]
        public void CannotGetDocWithExpiredCertificate()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), serverAdmin: true);
            var userCert = CreateAndPutExpiredClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });
            
            using (var store = GetDocumentStore(adminCertificate: adminCert, userCertificate: userCert, modifyName: s => dbName))
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
                attr.Path.Contains("/admin/") && (attr.RequiredAuthorization != AuthorizationStatus.ServerAdmin &&
                                                  attr.RequiredAuthorization != AuthorizationStatus.DatabaseAdmin));
            Assert.Empty(routes);
        }
        
        [Fact]
        public void AllAdminAuthorizationStatusHaveCorrectRoutes()
        {
            var routes = RouteScanner.Scan(attr =>
                !attr.Path.Contains("/admin/") && (attr.RequiredAuthorization == AuthorizationStatus.ServerAdmin ||
                                                  attr.RequiredAuthorization == AuthorizationStatus.DatabaseAdmin));
            Assert.Empty(routes);
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

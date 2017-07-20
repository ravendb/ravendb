// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server.Operations.Certificates;
using Raven.Server;
using Raven.Server.Routing;
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
            var serverCertificateHolder = RavenServer.LoadCertificate(serverCertPath, null);

            var clientCertificate = CertificateUtils.CreateSelfSignedExpiredClientCertificate("expired client cert", serverCertificateHolder);

            using (var store = GetDocumentStore(certificate: serverCertificate))
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
            var serverCertificate = new X509Certificate2(serverCertPath); // W.A. need to fix GetDocumentStore()

            var clientCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                ["CanGetDocWithValidPermission"] = DatabaseAccess.ReadWrite
            });
            using (var store = GetDocumentStore(certificate: serverCertificate, modifyName:(s => "CanGetDocWithValidPermission")))
            {
                store.Certificate = clientCert; // W.A.

                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
            }
        }

        [Fact]
        public void CannotGetDocWithoutCertificate()
        {
            var serverCertPath = SetupServerAuthentication();
            var clientCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                ["CannotGetDocWithoutCertificate"] = DatabaseAccess.ReadWrite
            });
            Assert.Throws<AuthorizationException>(() =>
            {
                using (var store = GetDocumentStore(certificate: null, modifyName: (s => "CannotGetDocWithoutCertificate")))
                {
                    StoreSampleDoc(store, "test/1");
                    using (var session = store.OpenSession())
                        session.Load<dynamic>("test/1");
                }
            });

        }

        [Fact]
        public void CannotGetDocWithInvalidPermission()
        {
            var serverCertPath = SetupServerAuthentication();
            var serverCertificate = new X509Certificate2(serverCertPath); // W.A. need to fix GetDocumentStore()
            var clientCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                ["OtherDB"] = DatabaseAccess.ReadWrite
            });

            Assert.Throws<AuthorizationException>(() =>
            {
                using (var store = GetDocumentStore(certificate: serverCertificate, modifyName: (s => "CannotGetDocWithInvalidPermission")))
                {
                    store.Certificate = clientCert; // W.A.

                    StoreSampleDoc(store, "test/1");
                    using (var session = store.OpenSession())
                        session.Load<dynamic>("test/1");
                }
            });

        }

        [Fact]
        public void CannotGetDocWhenNotUsingHttps()
        {
            Assert.Throws<System.InvalidOperationException>(() =>
            {
                var serverCertPath = SetupServerAuthentication(serverUrl: "http://" + Environment.MachineName + ":8080");
                var serverCertificate = new X509Certificate2(serverCertPath); // W.A. need to fix GetDocumentStore()

                var clientCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
                {
                    ["CannotGetDocWhenNotUsingHttps"] = DatabaseAccess.ReadWrite
                });
                using (var store = GetDocumentStore(certificate: serverCertificate, modifyName: s => "CannotGetDocWhenNotUsingHttps"))
                {
                    store.Certificate = clientCert; // W.A.
                    StoreSampleDoc(store, "test/1");
                    using (var session = store.OpenSession())
                        session.Load<dynamic>("test/1");
                }
            });
        }

        [Fact]
        public void CannotGetDocWithInvalidDbNamePermission()
        {
            var e = Assert.Throws<RavenException>(() =>
            {
                var serverCertPath = SetupServerAuthentication();
                var serverCertificate = new X509Certificate2(serverCertPath); // W.A. need to fix GetDocumentStore()
                var clientCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
                {
                    ["CannotGetDocWithIr*&^%$#W$mePermission"] = DatabaseAccess.ReadWrite
                });
                
                using (var store = GetDocumentStore(certificate: serverCertificate, modifyName: s => "CannotGetDocWithInvalidDbNamePermission"))
                {
                    store.Certificate = clientCert; // W.A.
                    StoreSampleDoc(store, "test/1");
                    using (var session = store.OpenSession())
                        session.Load<dynamic>("test/1");
                }
            });
            Assert.IsType<InvalidOperationException>(e.InnerException);
        }

        [Fact]
        public void CannotGetDocWithExpiredCertificate()
        {
            var serverCertPath = SetupServerAuthentication();
            var serverCertificate = new X509Certificate2(serverCertPath); // W.A. need to fix GetDocumentStore()

            var clientCert = CreateAndPutExpiredClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                ["CannotGetDocWithExpiredCertificate"] = DatabaseAccess.ReadWrite
            });

            Assert.Throws<AuthorizationException>(() =>
            {
                using (var store = GetDocumentStore(certificate: serverCertificate, modifyName: s => "CannotGetDocWithExpiredCertificate"))
                {
                    store.Certificate = clientCert; // W.A.
                    StoreSampleDoc(store, "test/1");
                    using (var session = store.OpenSession())
                        session.Load<dynamic>("test/1");
                }
            });
        }

        [Fact]
        public void AllAdminRoutesHaveCorrectAuthorizationStatus()
        {
            var routes = RouteScanner.Scan(attr => attr.Path.Contains("/admin/") && attr.RequiredAuthorization != AuthorizationStatus.ServerAdmin);
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
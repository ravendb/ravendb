// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Server.Operations.Certificates;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Authentication
{
    public class AuthenticationBasicTests : RavenTestBase
    {
        public X509Certificate2 CreateAndPutExpiredClientCertificate(string serverCertPath, IEnumerable<string> permissions, bool serverAdmin = false)
        {
            var serverCertificate = new X509Certificate2(serverCertPath);
            var serverCertificateHolder = RavenServer.LoadCertificate(serverCertPath, null);

            var clientCertificate = CertificateUtils.CreateSelfSignedExpiredClientCertificate("expired client cert", serverCertificateHolder);

            using (var store = GetDocumentStore(certificate: serverCertificate))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new PutClientCertificateOperation(Convert.ToBase64String(clientCertificate.Export(X509ContentType.Cert)), permissions, serverAdmin)
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
            var clientCert = AskServerForClientCertificate(serverCertPath, new[] {"Northwind"});

            using (var store = GetDocumentStore(certificate: clientCert, modifyName:(s => "Northwind")))
            {
                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
            }
        }
        
        [Fact]
        public void CannotGetDocWithInvalidPermission()
        {
            var serverCertPath = SetupServerAuthentication();
            var clientCert = AskServerForClientCertificate(serverCertPath, new[] {"Southwind"});

            Assert.Throws<AuthorizationException>(() =>
            {
                using (var store = GetDocumentStore(certificate: clientCert, modifyName: (s => "Northwind")))
                {
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
                var clientCert = AskServerForClientCertificate(serverCertPath, new[] { "Northwind" });
                
                using (var store = GetDocumentStore(certificate: clientCert, modifyName: s => "Northwind"))
                {
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
                var clientCert = AskServerForClientCertificate(serverCertPath, new[] {"North?ind"});
                
                using (var store = GetDocumentStore(certificate: clientCert, modifyName: s => "Northwind"))
                {
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
            var clientCert = CreateAndPutExpiredClientCertificate(serverCertPath, new[] {"Northwind"});

            Assert.Throws<AuthorizationException>(() =>
            {
                using (var store = GetDocumentStore(certificate: clientCert, modifyName: s => "Northwind"))
                {
                    StoreSampleDoc(store, "test/1");
                    using (var session = store.OpenSession())
                        session.Load<dynamic>("test/1");
                }
            });
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
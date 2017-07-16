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
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Server.Operations.Certificates;
using Raven.Server.Config;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Authentication
{
    public class AuthenticationBasicTests : RavenTestBase
    {
        private string _serverCertPath;
        private void SetupServerAuthentication()
        {
            _serverCertPath = GenerateAndSaveSelfSignedCertificate();
            DoNotReuseServer(new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = _serverCertPath,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrl)] = "https://" + Environment.MachineName + ":8080",
                [RavenConfiguration.GetKey(x => x.Security.AuthenticationEnabled)] = "True"
            });
        }

        public X509Certificate2 AskServerForClientCertificate(IEnumerable<string> permissions, bool serverAdmin = false)
        {
            var serverCertificate = new X509Certificate2(_serverCertPath);
            X509Certificate2 clientCertificate;

            using (var store = GetDocumentStore(certificate: serverCertificate))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new CreateClientCertificateOperation("client certificate", permissions, serverAdmin)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                    clientCertificate = new X509Certificate2(command.Result.RawData);
                }
            }
            return clientCertificate;
        }

        [Fact]
        public void CanGetDocWithValidPermission()
        {
            SetupServerAuthentication();
            var clientCert = AskServerForClientCertificate(new[] {"Northwind"});

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
            SetupServerAuthentication();
            var clientCert = AskServerForClientCertificate(new[] {"Southwind"});

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
        public void CannotGetDocWithInvalidDbName()
        {
            SetupServerAuthentication();
            var clientCert = AskServerForClientCertificate(new[] {"North?ind"});

            Assert.Throws<InvalidOperationException>(() =>
            {
                using (var store = GetDocumentStore(certificate: clientCert, modifyName: (s => "Northwind")))
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
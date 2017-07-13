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
            //temporary workaround
            _serverCertPath = "c:\\work\\temp\\iftah-pc.pfx"; 
            //_serverCertPath = GenerateAndSaveSelfSignedCertificate();
            DoNotReuseServer(new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = _serverCertPath,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = "1234",
                [RavenConfiguration.GetKey(x => x.Core.ServerUrl)] = "https://" + Environment.MachineName + ":8080",
                [RavenConfiguration.GetKey(x => x.Security.AuthenticationEnabled)] = "True"
            });
        }

        public X509Certificate2 GetClientCertificate(IEnumerable<string> permissions, bool serverAdmin = false)
        {
            var serverCertificate = new X509Certificate2(_serverCertPath);
            X509Certificate2 clientCertificate;

            using (var store = GetDocumentStore(certificate: serverCertificate))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetClientCertificateOperation("client certificate", permissions, serverAdmin, "1234")
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                    clientCertificate = new X509Certificate2(command.Result.RawData, "1234");
                }
            }
            return clientCertificate;
        }

        [Fact]
        public void CanGetDocWithValidPermission()
        {
            SetupServerAuthentication();
            var clientCert = GetClientCertificate(new[] {"Northwind"});

            using (var store = GetDocumentStore(certificate: clientCert, modifyName:(s => "Northwind")))
            {
                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
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
using System;
using System.Collections.Generic;
using System.DirectoryServices.AccountManagement;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Common.Attributes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3570 : RavenFilesTestBase
    {
        public RavenDB_3570()
        {
            FactIfWindowsAuthenticationIsAvailable.LoadCredentials();
        }

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
                        server.SystemDatabase.Documents.Put("Raven/Authorization/WindowsSettings", null,
                                                  RavenJObject.FromObject(new WindowsAuthDocument
                                                  {
                                                      RequiredUsers = new List<WindowsAuthData>
                                                      {
                                                          new WindowsAuthData
                                                          {
                                                              Name = string.Format("{0}\\{1}", null, FactIfWindowsAuthenticationIsAvailable.Username),
                                                              Enabled = true,
                                                              Databases = new List<ResourceAccess>
                                                              {
                                                                  new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true}, // required to create file system
                                                                  new ResourceAccess {TenantId = fileSystemName}
                                                              }
                                                          }
                                                      }
                                                  }), new RavenJObject(), null);
        }

        //requires admin context
        [Fact]
        public void RavenFSWithWindowsCredentialsInConnectionStringShouldWork()
        {
            this.Invoking(x =>
            {
                using (NewStore(enableAuthentication: true, connectionStringName: "RavenFS"))
                {
                }
            }).ShouldThrow<ErrorResponseException>().Where(x => x.StatusCode == HttpStatusCode.Unauthorized);

            this.Invoking(x =>
            {
                using (NewStore(enableAuthentication: true))
                {
                }
            }).ShouldNotThrow<Exception>();
        }
    }
}

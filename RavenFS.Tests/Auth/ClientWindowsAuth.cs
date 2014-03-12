// -----------------------------------------------------------------------
//  <copyright file="ClientWindowsAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace RavenFS.Tests.Auth
{
    public class ClientWindowsAuth : RavenFsTestBase
    {
        private string username = "local_user_test";

        private string password = "local_user_test";

        private string domain = "local_machine_name_test";

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            server.SystemDatabase.Put("Raven/Authorization/WindowsSettings", null,
                                      RavenJObject.FromObject(new WindowsAuthDocument
                                      {
                                          RequiredUsers = new List<WindowsAuthData>
                                          {
                                              new WindowsAuthData()
                                              {
                                                  Name = string.Format("{0}\\{1}", domain, username),
                                                  Enabled = true,
                                                  Databases = new List<DatabaseAccess>
                                                  {
                                                      new DatabaseAccess {TenantId = Constants.SystemDatabase, Admin = true} // required to create file system
                                                  },
                                                  FileSystems = new List<FileSystemAccess>()
                                                  {
                                                      new FileSystemAccess() {TenantId = fileSystemName}
                                                  }
                                              }
                                          }
                                      }), new RavenJObject(), null);
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task CanUploadAndDownload()
        {
            var client = NewClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain));

            var ms = new MemoryStream(new byte[]{1, 2, 4});

            await client.UploadAsync("ms.bin", ms);

            var result = new MemoryStream();

            await client.DownloadAsync("ms.bin", result);

            ms.Position = 0;
            result.Position = 0;

            Assert.Equal(ms.GetMD5Hash(), result.GetMD5Hash());
        }
    }
}
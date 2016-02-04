// -----------------------------------------------------------------------
//  <copyright file="ClientWindowsAuth2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Common.Attributes;
using Raven.Tests.FileSystem.Synchronization.IO;
using Raven.Tests.Helpers.Util;
using Xunit;

namespace Raven.Tests.FileSystem.Auth
{
    public class ClientWindowsAuth2 : RavenFilesTestWithLogs
    {
        protected override void ModifyStore(FilesStore store)
        {
            FactIfWindowsAuthenticationIsAvailable.LoadCredentials();
            ConfigurationHelper.ApplySettingsToConventions(store.Conventions);

            base.ModifyStore(store);
        }

        public ClientWindowsAuth2()
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
                                              new WindowsAuthData()
                                              {
                                                  Name = string.Format("{0}\\{1}", FactIfWindowsAuthenticationIsAvailable.User.Domain, FactIfWindowsAuthenticationIsAvailable.User.UserName),
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

        [Fact]
        public async Task ShouldThrowWhenWindowsDocumentDoesNotContainFileSystem()
        {
            // in this test be careful if the specified credentials belong to admin user or not
            // to make this test pass you need to specify the credentials of a user who isn't an admin on this machine

            var client = NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.User.UserName, FactIfWindowsAuthenticationIsAvailable.User.Password, FactIfWindowsAuthenticationIsAvailable.User.Domain));
            var server = GetServer();

            await client.UploadAsync("abc.bin", new MemoryStream(1));

            using (var anotherClient = new AsyncFilesServerClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "ShouldThrow_WindowsDocumentDoesNotContainsThisFS",
                conventions: client.Conventions,
                credentials: new OperationCredentials(null, new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.User.UserName, FactIfWindowsAuthenticationIsAvailable.User.Password, FactIfWindowsAuthenticationIsAvailable.User.Domain))))
            {
                await anotherClient.EnsureFileSystemExistsAsync(); // will pass because by using this api key we have access to <system> database

                ErrorResponseException errorResponse = null;

                try
                {
                    await anotherClient.UploadAsync("def.bin", new MemoryStream(1)); // should throw because a file system ShouldThrow_WindowsDocumentDoesNotContainsThisFS isn't added to ApiKeyDefinition
                }
                catch (ErrorResponseException ex)
                {
                    errorResponse = ex;
                }

                Assert.NotNull(errorResponse);
                Assert.Equal(HttpStatusCode.Forbidden, errorResponse.StatusCode);
            }
        }
    }
}
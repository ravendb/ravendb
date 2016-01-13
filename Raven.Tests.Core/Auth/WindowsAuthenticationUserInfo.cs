#if !DNXCORE50
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using System.Collections.Generic;
using System.Net;
using Raven.Client.Extensions;
using Raven.Tests.Common.Attributes;
using Raven.Tests.Helpers.Util;

using Xunit;

namespace Raven.Tests.Core.Auth
{
    public class WindowsAuthenticationUserInfo : RavenCoreTestBase
    {
        public WindowsAuthenticationUserInfo()
        {
            FactIfWindowsAuthenticationIsAvailable.LoadCredentials();
        }

        [Fact]
        public void GetUserInfoAndPermissionsWindowsAuthentication()
        {
            Raven.Database.Server.Security.Authentication.EnableOnce();
            Server.Configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
            Server.SystemDatabase.Documents.Put(
                "Raven/Authorization/WindowsSettings",
                null,
                RavenJObject.FromObject(new WindowsAuthDocument
                {
                    RequiredUsers = new List<WindowsAuthData>
                    {
                        new WindowsAuthData
                        {
                            Name = string.Format("{0}\\{1}", FactIfWindowsAuthenticationIsAvailable.Admin.Domain, FactIfWindowsAuthenticationIsAvailable.Admin.UserName),
                            Enabled = true,
                            Databases = new List<ResourceAccess>
                            {
                                new ResourceAccess {TenantId = "Foo", Admin = true},
                                new ResourceAccess {TenantId = "db2", ReadOnly = true},
                                new ResourceAccess {TenantId = "db3", Admin = false},
                            }
                        }
                    }
                }), new RavenJObject(), null);

            using (var store = new DocumentStore
            {
                Credentials = new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.Admin.UserName, FactIfWindowsAuthenticationIsAvailable.Admin.Password, FactIfWindowsAuthenticationIsAvailable.Admin.Domain),
                Url = Server.SystemDatabase.ServerUrl
            })
            {
                ConfigurationHelper.ApplySettingsToConventions(store.Conventions);

                store.Initialize();

                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "Foo",
                        Settings =
                        {
                            {"Raven/DataDir", "Foo"}
                        }
                    });
                store.DatabaseCommands.EnsureDatabaseExists("Foo");

                var info = store.DatabaseCommands.GetUserInfo();

                Assert.Equal(3, info.Databases.Count);

                Assert.Equal("Foo", info.Databases[0].Database);
                Assert.Equal("db2", info.Databases[1].Database);
                Assert.Equal("db3", info.Databases[2].Database);

                Assert.True(info.Databases[0].IsAdmin);
                Assert.False(info.Databases[1].IsAdmin);
                Assert.False(info.Databases[2].IsAdmin);

                Assert.Equal(1, info.ReadOnlyDatabases.Count);

                var per = store.DatabaseCommands.GetUserPermission("Foo", readOnly: false);
                var isGrant = per.IsGranted;
                var res = per.Reason;
                Assert.True(isGrant);
                Assert.Equal("PUT allowed on " + "Foo" + " because user " + info.User + " has admin permissions", res);

                var per2 = store.DatabaseCommands.GetUserPermission("db2", readOnly: false);
                var isGrant2 = per2.IsGranted;
                var res2 = per2.Reason;
                Assert.False(isGrant2);
                Assert.Equal("PUT rejected on" + "db2" + "because user" + info.User + "has ReadOnly permissions", res2);

                var per3 = store.DatabaseCommands.GetUserPermission("db3", readOnly: false);
                var isGrant3 = per3.IsGranted;
                var res3 = per3.Reason;
                Assert.True(isGrant3);
                Assert.Equal("PUT allowed on " + "db3" + " because user " + info.User + "has ReadWrite permissions", res3);
            }
        }
    }
}
#endif
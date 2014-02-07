// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1595.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1595 : RavenTest
    {
        private RavenDbServer server1;
        private DocumentStore store1;

        [Fact]
        public void ProperlyHandleDataDirectoryWhichEndsWithSlash()
        {
            server1 = CreateServer(8079, "D1");

            store1 = new DocumentStore
            {
                DefaultDatabase = "Northwind",
                Url = "http://localhost:8079"
            };

            store1.Initialize();

            store1.DatabaseCommands.GlobalAdmin.CreateDatabase(
                new DatabaseDocument
                {
                    Id = "Northwind",
                    Settings = { { "Raven/DataDir", @"~\D1\N" } }
                });

            Assert.True(
                Directory.Exists(Path.Combine(server1.SystemDatabase.Configuration.DataDirectory,
                                              "Databases//Northwind//")));


        }

        private RavenDbServer CreateServer(int port, string dataDirectory, bool removeDataDirectory = true)
        {
            Database.Server.NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);

            var serverConfiguration = new Database.Config.RavenConfiguration
            {
                AnonymousUserAccessMode = Database.Server.AnonymousUserAccessMode.Admin,
                DataDirectory = dataDirectory,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                RunInMemory = false,
                Port = port,
                DefaultStorageTypeName = "esent"
            };

            if (removeDataDirectory)
                IOExtensions.DeleteDirectory(serverConfiguration.DataDirectory);

            var server = new RavenDbServer(serverConfiguration);
            serverConfiguration.PostInit();

            return server;
        }
        public override void Dispose()
        {
            if (server1 != null)
            {
                server1.Dispose();
                IOExtensions.DeleteDirectory(server1.SystemDatabase.Configuration.DataDirectory);
            }

            if (store1 != null)
            {
                store1.Dispose();
            }
        }
    }

}
// -----------------------------------------------------------------------
//  <copyright file="CoreTestServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Raven.Database.Config;
using Raven.Database.Config.Settings;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;

namespace Raven.Tests.Core
{
    public class TestServerFixture : IDisposable
    {
        public const int Port = 8079;
        public const string ServerName = "Raven.Tests.Core.Server";

        public TestServerFixture()
        {
            var configuration = new RavenConfiguration();
            configuration.Core.Port = Port;
            configuration.Server.Name = ServerName;
            configuration.Core.RunInMemory = configuration.DefaultStorageTypeName == InMemoryRavenConfiguration.VoronTypeName;
            configuration.Core.DataDirectory = Path.Combine(configuration.Core.DataDirectory, "Tests");
            configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);
            configuration.Storage.AllowOn32Bits = true;

            IOExtensions.DeleteDirectory(configuration.Core.DataDirectory);

            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(Port);

            Server = new RavenDbServer(configuration)
            {
                UseEmbeddedHttpServer = true,
                RunInMemory = true
            }.Initialize();
        }

        public RavenDbServer Server { get; private set; }

        public void Dispose()
        {
            Server.Dispose();
        }
    }
}

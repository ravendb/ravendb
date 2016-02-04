// -----------------------------------------------------------------------
//  <copyright file="ServerRunner.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Microsoft.Owin.Hosting;

using Raven.Database.Server;

namespace Raven.Tests.Server.Runner
{
    public class ServerRunner : IDisposable
    {
        private readonly IDisposable server;

        public ServerRunner(int port)
        {
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
            server = WebApp.Start<Startup>(string.Format("http://+:{0}/", port));
        }

        public void Dispose()
        {
            if (server != null)
                server.Dispose();
        }
    }
}

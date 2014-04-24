// -----------------------------------------------------------------------
//  <copyright file="CoreTestServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Database.Config;
using Raven.Server;

namespace Raven.Tests.Core
{
	public class TestServerFixture : IDisposable
	{
		public const int Port = 8079;
		public const string ServerName = "Raven.Tests.Core.Server";

		public TestServerFixture()
		{
			Server = new RavenDbServer(new RavenConfiguration()
			{
				Port = Port,
				ServerName = ServerName
			})
			{
				RunInMemory = true,
				UseEmbeddedHttpServer = true
			}.Initialize();
		}

		public RavenDbServer Server { get; private set; }

		public void Dispose()
		{
			Server.Dispose();
		}
	}
}
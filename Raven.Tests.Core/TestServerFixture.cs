// -----------------------------------------------------------------------
//  <copyright file="CoreTestServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Server;

namespace Raven.Tests.Core
{
	public class TestServerFixture : IDisposable
	{
		public TestServerFixture()
		{
			Server = new RavenDbServer { RunInMemory = true }.Initialize();
		}

		public RavenDbServer Server { get; private set; }

		public void Dispose()
		{
			Server.Dispose();
		}
	}
}
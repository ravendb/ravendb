// -----------------------------------------------------------------------
//  <copyright file="RavenRaftEngine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Rachis;

namespace Raven.Database.Raft
{
	public class RavenRaftEngine : IDisposable
	{
		public RaftEngine Engine { get; private set; }

		public RaftHttpClient Client { get; private set; }

		public RavenRaftEngine(RaftEngine engine)
		{
			Engine = engine;
			Client = new RaftHttpClient(engine);
		}

		public void Dispose()
		{
			if (Engine != null)
				Engine.Dispose();
		}
	}
}
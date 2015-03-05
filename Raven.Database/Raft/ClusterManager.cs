// -----------------------------------------------------------------------
//  <copyright file="ClusterManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Rachis;

namespace Raven.Database.Raft
{
	public class ClusterManager : IDisposable
	{
		public RaftEngine Engine { get; private set; }

		public ClusterManagementHttpClient Client { get; private set; }

		public ClusterManager(RaftEngine engine)
		{
			Engine = engine;
			Client = new ClusterManagementHttpClient(engine);
		}

		public void Dispose()
		{
			if (Engine != null)
				Engine.Dispose();
		}
	}
}
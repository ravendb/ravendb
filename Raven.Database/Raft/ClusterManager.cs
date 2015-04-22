// -----------------------------------------------------------------------
//  <copyright file="ClusterManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Rachis;

using Raven.Database.Impl;

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
			var aggregator = new ExceptionAggregator("ClusterManager disposal error.");

			aggregator.Execute(() =>
			{
				if (Client != null)
					Client.Dispose();
			});

			aggregator.Execute(() =>
			{
				if (Engine != null)
					Engine.Dispose();
			});

			aggregator.ThrowIfNeeded();
		}
	}
}
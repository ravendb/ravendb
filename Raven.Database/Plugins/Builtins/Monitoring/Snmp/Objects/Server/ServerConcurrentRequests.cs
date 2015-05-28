// -----------------------------------------------------------------------
//  <copyright file="ServerConcurrentRequests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerConcurrentRequests : ScalarObjectBase<Gauge32>
	{
		private readonly RequestManager requestManager;

		public ServerConcurrentRequests(RequestManager requestManager)
			: base("1.6.1")
		{
			this.requestManager = requestManager;
		}

		protected override Gauge32 GetData()
		{
			return new Gauge32(requestManager.NumberOfConcurrentRequests);
		}
	}
}
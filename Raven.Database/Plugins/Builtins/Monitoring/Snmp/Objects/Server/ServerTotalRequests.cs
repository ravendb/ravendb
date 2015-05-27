// -----------------------------------------------------------------------
//  <copyright file="ServerTotalRequests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerTotalRequests : ScalarObjectBase<Integer32>
	{
		private readonly RequestManager requestManager;

		public ServerTotalRequests(RequestManager requestManager)
			: base("1.6.2")
		{
			this.requestManager = requestManager;
		}

		protected override Integer32 GetData()
		{
			return new Integer32(requestManager.NumberOfRequests);
		}
	}
}
// -----------------------------------------------------------------------
//  <copyright file="ServerTotalRequests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerTotalRequests : ScalarObjectBase
	{
		private readonly RequestManager requestManager;

		public ServerTotalRequests(RequestManager requestManager)
			: base("1.1.6.2")
		{
			this.requestManager = requestManager;
		}

		public override ISnmpData Data
		{
			get { return new Integer32(requestManager.NumberOfRequests); }
			set { throw new AccessFailureException(); }
		}
	}
}
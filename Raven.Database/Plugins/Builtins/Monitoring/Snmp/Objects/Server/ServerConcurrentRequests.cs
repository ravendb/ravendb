// -----------------------------------------------------------------------
//  <copyright file="ServerConcurrentRequests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerConcurrentRequests : ScalarObject
	{
		private readonly RequestManager requestManager;

		public ServerConcurrentRequests(RequestManager requestManager)
			: base(new ObjectIdentifier("1.1.6.1"))
		{
			this.requestManager = requestManager;
		}

		public override ISnmpData Data
		{
			get { return new Integer32((int)requestManager.NumberOfConcurrentRequests); }
			set { throw new AccessFailureException(); }
		}
	}
}
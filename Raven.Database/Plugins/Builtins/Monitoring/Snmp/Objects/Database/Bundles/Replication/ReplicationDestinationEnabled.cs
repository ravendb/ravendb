// -----------------------------------------------------------------------
//  <copyright file="ReplicationDestinationEnabled.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Lextm.SharpSnmpLib;

using Raven.Bundles.Replication.Tasks;
using Raven.Client.Connection;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Bundles.Replication
{
	public class ReplicationDestinationEnabled : ReplicationDestinationScalarObjectBase
	{
		public ReplicationDestinationEnabled(string databaseName, DatabasesLandlord landlord, int databaseIndex, string destinationUrl, int destinationIndex)
			: base(databaseName, landlord, databaseIndex, destinationUrl, destinationIndex, "1")
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			var task = database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			if (task == null)
				return Null;

			var destinations = task.GetReplicationDestinations(destination => string.Equals(destination.Url.ForDatabase(destination.Database), DestinationUrl));
			if (destinations == null || destinations.Length == 0)
				return new OctetString(false.ToString());

			return new OctetString(true.ToString());
		}

		public override ISnmpData GetData(DocumentDatabase database, ReplicationTask task, ReplicationStrategy destination)
		{
			throw new NotSupportedException();
		}
	}
}
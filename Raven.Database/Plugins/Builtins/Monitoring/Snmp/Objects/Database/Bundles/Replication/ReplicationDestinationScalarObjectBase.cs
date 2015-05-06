// -----------------------------------------------------------------------
//  <copyright file="ReplicationBundleScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Lextm.SharpSnmpLib;

using Raven.Bundles.Replication.Tasks;
using Raven.Client.Connection;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Bundles.Replication
{
	public abstract class ReplicationDestinationScalarObjectBase : DatabaseBundleScalarObjectBase
	{
		protected string DestinationUrl { get; private set; }

		protected ReplicationDestinationScalarObjectBase(string databaseName, DatabasesLandlord landlord, int databaseIndex, string destinationUrl, int destinationIndex, string dots)
			: base(databaseName, "Replication", landlord, databaseIndex, 1, string.Format("2.{0}.{1}", destinationIndex, dots))
		{
			DestinationUrl = destinationUrl;
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			var task = database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			if (task == null)
				return Null;

			var destinations = task.GetReplicationDestinations(destination => string.Equals(destination.Url.ForDatabase(destination.Database), DestinationUrl));
			if (destinations == null || destinations.Length == 0) 
				return Null;

			return GetData(database, task, destinations[0]);
		}

		public abstract ISnmpData GetData(DocumentDatabase database, ReplicationTask task, ReplicationStrategy destination);
	}
}
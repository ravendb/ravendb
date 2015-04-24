// -----------------------------------------------------------------------
//  <copyright file="DatabaseStaleIndexes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseStaleIndexes : ScalarObject
	{
		private readonly DocumentDatabase database;

		public DatabaseStaleIndexes(DocumentDatabase database, int index)
			: base("1.5.2.{0}.1.3", index)
		{
			this.database = database;
		}

		public override ISnmpData Data
		{
			get { return new Gauge32(GetCount()); }
			set { throw new AccessFailureException(); }
		}

		private long GetCount()
		{
			return database.IndexStorage.Indexes.Count(indexId => database.IndexStorage.IsIndexStale(indexId, database.LastCollectionEtags));
		}
	}
}
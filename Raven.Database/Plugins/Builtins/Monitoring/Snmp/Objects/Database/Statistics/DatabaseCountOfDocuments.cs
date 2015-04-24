// -----------------------------------------------------------------------
//  <copyright file="DatabaseCountOfDocuments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseCountOfDocuments : ScalarObject
	{
		private readonly DocumentDatabase database;

		public DatabaseCountOfDocuments(DocumentDatabase database, int index)
			: base("1.5.2.{0}.1.6", index)
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
			var count = 0L;
			database.TransactionalStorage.Batch(actions =>
			{
				count = actions.Documents.GetDocumentsCount();
			});

			return count;
		}
	}
}
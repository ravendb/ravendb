// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects
{
	public abstract class DatabaseIndexScalarObjectBase<TData> : DatabaseScalarObjectBase<TData>
		where TData : ISnmpData
	{
		protected readonly string IndexName;

		protected IndexDefinition IndexDefinition { get; private set; }

		protected DatabaseIndexScalarObjectBase(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex, string dots)
			: base(databaseName, landlord, string.Format("5.2.{0}.4.{{0}}.{1}", databaseIndex, dots), indexIndex)
		{
			IndexName = indexName;
		}

		public override ISnmpData Data
		{
			get
			{
				if (Landlord.IsDatabaseLoaded(DatabaseName))
				{
					var database = Landlord.GetDatabaseInternal(DatabaseName).Result;
					IndexDefinition = database.IndexDefinitionStorage.GetIndexDefinition(IndexName);
					if (IndexDefinition != null)
						return GetData(database);
				}

				return DefaultValue();
			}
		}

		protected IndexStats GetIndexStats(DocumentDatabase database)
		{
			IndexStats stats = null;
			database.TransactionalStorage.Batch(actions =>
			{
				stats = actions.Indexing.GetIndexStats(IndexDefinition.IndexId);
			});

			return stats;
		}
	}
}
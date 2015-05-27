// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexExists.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Indexes
{
	public class DatabaseIndexExists : DatabaseIndexScalarObjectBase<OctetString>
	{
		public DatabaseIndexExists(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
			: base(databaseName, indexName, landlord, databaseIndex, indexIndex, "1")
		{
		}

		public override ISnmpData Data
		{
			get
			{
				if (Landlord.IsDatabaseLoaded(DatabaseName))
				{
					var database = Landlord.GetDatabaseInternal(DatabaseName).Result;
					var exists = database.IndexDefinitionStorage.Contains(IndexName);

					return new OctetString(exists.ToString(CultureInfo.InvariantCulture));
				}

				return DefaultValue();
			}
		}

		protected override OctetString GetData(DocumentDatabase database)
		{
			throw new NotSupportedException();
		}
	}
}
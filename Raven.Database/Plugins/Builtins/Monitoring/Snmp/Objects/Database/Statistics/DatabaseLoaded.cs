// -----------------------------------------------------------------------
//  <copyright file="DatabaseLoaded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseLoaded : DatabaseScalarObjectBase<OctetString>
	{
		private readonly string databaseName;

		public DatabaseLoaded(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "5.2.{0}.1.13", index)
		{
			this.databaseName = databaseName;
		}

		public override ISnmpData Data
		{
			get
			{
				return new OctetString(Landlord.IsDatabaseLoaded(databaseName).ToString(CultureInfo.InvariantCulture));
			}
		}

		protected override OctetString GetData(DocumentDatabase database)
		{
			throw new NotSupportedException();
		}
	}
}
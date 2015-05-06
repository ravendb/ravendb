// -----------------------------------------------------------------------
//  <copyright file="BundleScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects
{
	public abstract class DatabaseBundleScalarObjectBase : DatabaseScalarObjectBase
	{
		public string BundleName { get; private set; }

		protected DatabaseBundleScalarObjectBase(string databaseName, string bundleName, DatabasesLandlord landlord, int databaseIndex, int bundleIndex, string dots)
			: base(databaseName, landlord, string.Format("5.2.{0}.6.{{0}}.{1}", databaseIndex, dots), bundleIndex)
		{
			BundleName = bundleName;
		}

		public override ISnmpData Data
		{
			get
			{
				if (Landlord.IsDatabaseLoaded(DatabaseName))
				{
					var database = Landlord.GetDatabaseInternal(DatabaseName).Result;
					var isBundleActive = database.Configuration.ActiveBundles.Any(x => x.Equals(BundleName, StringComparison.OrdinalIgnoreCase));
					if (isBundleActive)
						return GetData(database);
				}

				return Null;
			}
		}
	}
}
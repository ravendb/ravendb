// -----------------------------------------------------------------------
//  <copyright file="DatabaseScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects
{
	public abstract class DatabaseScalarObjectBase<TData> : ScalarObjectBase<TData>
		where TData : ISnmpData
	{
		protected readonly string DatabaseName;

		protected readonly DatabasesLandlord Landlord;

		protected DatabaseScalarObjectBase(string databaseName, DatabasesLandlord landlord, string dots, int index)
			: base(dots, index)
		{
			DatabaseName = databaseName;
			Landlord = landlord;
		}

		protected abstract TData GetData(DocumentDatabase database);

		protected override TData GetData()
		{
			if (Landlord.IsDatabaseLoaded(DatabaseName))
				return GetData(Landlord.GetDatabaseInternal(DatabaseName).Result);

			return default(TData);
		}
	}
}
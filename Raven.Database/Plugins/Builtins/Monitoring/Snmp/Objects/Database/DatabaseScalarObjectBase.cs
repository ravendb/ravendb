// -----------------------------------------------------------------------
//  <copyright file="DatabaseScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database
{
	public abstract class DatabaseScalarObjectBase : ScalarObject
	{
		private static readonly Null Null = new Null();

		private readonly string databaseName;

		protected readonly DatabasesLandlord Landlord;

		protected DatabaseScalarObjectBase(string databaseName, DatabasesLandlord landlord, ObjectIdentifier identifier)
			: base(identifier)
		{
			this.databaseName = databaseName;
			Landlord = landlord;
		}

		protected DatabaseScalarObjectBase(string databaseName, DatabasesLandlord landlord, string dots, int index)
			: base(dots, index)
		{
			this.databaseName = databaseName;
			Landlord = landlord;
		}

		protected abstract ISnmpData GetData(DocumentDatabase database);

		public override ISnmpData Data
		{
			get
			{
				if (Landlord.IsDatabaseLoaded(databaseName)) 
					return GetData(Landlord.GetDatabaseInternal(databaseName).Result);

				return Null;
			}

			set
			{
				throw new AccessFailureException();
			}
		}
	}
}
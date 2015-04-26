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

		private readonly DatabasesLandlord landlord;

		protected DatabaseScalarObjectBase(string databaseName, DatabasesLandlord landlord, ObjectIdentifier identifier)
			: base(identifier)
		{
			this.databaseName = databaseName;
			this.landlord = landlord;
		}

		protected DatabaseScalarObjectBase(string databaseName, DatabasesLandlord landlord, string dots, int index)
			: base(dots, index)
		{
			this.databaseName = databaseName;
			this.landlord = landlord;
		}

		protected abstract ISnmpData GetData(DocumentDatabase database);

		public override ISnmpData Data
		{
			get
			{
				if (landlord.IsDatabaseLoaded(databaseName)) 
					return GetData(landlord.GetDatabaseInternal(databaseName).Result);

				return Null;
			}

			set
			{
				throw new AccessFailureException();
			}
		}
	}
}
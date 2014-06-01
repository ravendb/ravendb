// -----------------------------------------------------------------------
//  <copyright file="SchemaUpdateBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

using Raven.Database.Server.RavenFS.Storage.Voron.Impl;

using Voron;

namespace Raven.Database.Server.RavenFS.Storage.Voron.Schema.Updates
{
	public abstract class SchemaUpdateBase : ISchemaUpdate
	{
		public abstract string FromSchemaVersion { get; }

		public abstract string ToSchemaVersion { get; }

		public abstract void Update(TableStorage tableStorage, Action<string> output);

		public void UpdateSchemaVersion(TableStorage tableStorage,  Action<string> output)
		{
			using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.ReadTree(Tables.Details.TableName).Add("schema_version", ToSchemaVersion);
				tx.Commit();
			}

			tableStorage.SetDatabaseIdAndSchemaVersion(tableStorage.Id, ToSchemaVersion);
		}
	}
}
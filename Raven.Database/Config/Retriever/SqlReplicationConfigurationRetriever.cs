// -----------------------------------------------------------------------
//  <copyright file="SqlReplicationConfigurationRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Database.Bundles.SqlReplication;

namespace Raven.Database.Config.Retriever
{
	public class SqlReplicationConfigurationRetriever : ConfigurationRetrieverBase<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>
	{
		public SqlReplicationConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
			: base(systemDatabase, localDatabase)
		{
		}

		protected override SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> ApplyGlobalDocumentToLocal(SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> global, SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> local)
		{
			foreach (var localConnection in local.PredefinedConnections)
			{
				localConnection.IsLocal = true;
				localConnection.IsGlobal = false;
			}

			foreach (var globalConnection in global.PredefinedConnections)
			{
				globalConnection.IsLocal = false;
				globalConnection.IsGlobal = true;

				var localConnectionExists = local.PredefinedConnections.Any(x => string.Equals(x.Name, globalConnection.Name, StringComparison.OrdinalIgnoreCase));
				if (localConnectionExists)
					continue;

				local.PredefinedConnections.Add(globalConnection);
			}

			return local;
		}

		protected override SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> ConvertGlobalDocumentToLocal(SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> global)
		{
			foreach (var localConnection in global.PredefinedConnections)
			{
				localConnection.IsLocal = false;
				localConnection.IsGlobal = true;
			}

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key)
		{
			return Constants.Global.SqlReplicationConnectionsDocumentName;
		}
	}
}
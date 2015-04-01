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
		protected override SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> ApplyGlobalDocumentToLocal(SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> global, SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> local, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			local.PredefinedConnections = local.PredefinedConnections ?? global.PredefinedConnections;

			foreach (var localConnection in local.PredefinedConnections)
			{
				localConnection.HasLocal = true;
			}

			foreach (var globalConnection in global.PredefinedConnections)
			{
				globalConnection.HasGlobal = true;

				var localConnection = local.PredefinedConnections.FirstOrDefault(x => string.Equals(x.Name, globalConnection.Name, StringComparison.OrdinalIgnoreCase));
				if (localConnection != null)
				{
					localConnection.HasGlobal = true;
					continue;
				}

				local.PredefinedConnections.Add(globalConnection);
			}

			return local;
		}

		protected override SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> ConvertGlobalDocumentToLocal(SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> global, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			foreach (var localConnection in global.PredefinedConnections)
			{
				localConnection.HasLocal = false;
				localConnection.HasGlobal = true;
			}

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return Constants.Global.SqlReplicationConnectionsDocumentName;
		}
	}
}
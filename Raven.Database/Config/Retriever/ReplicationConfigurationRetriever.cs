// -----------------------------------------------------------------------
//  <copyright file="ReplicationConfigurationRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.Database.Config.Retriever
{
	internal class ReplicationConfigurationRetriever : ConfigurationRetrieverBase<ReplicationDocument>
	{
		public ReplicationConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
			: base(systemDatabase, localDatabase)
		{
		}

		protected override ReplicationDocument ApplyGlobalDocumentToLocal(ReplicationDocument global, ReplicationDocument local)
		{
			local.ClientConfiguration = local.ClientConfiguration ?? global.ClientConfiguration;

			foreach (var globalDestination in global.Destinations)
			{
				var localDestination = local.Destinations.FirstOrDefault(x => string.Equals(x.Url, globalDestination.Url, StringComparison.OrdinalIgnoreCase) && string.Equals(x.Database, LocalDatabase.Name, StringComparison.OrdinalIgnoreCase));
				if (localDestination != null)
					continue;

				globalDestination.Database = LocalDatabase.Name;
				local.Destinations.Add(globalDestination);
			}

			return local;
		}

		protected override ReplicationDocument ConvertGlobalDocumentToLocal(ReplicationDocument global)
		{
			global.Id = Constants.RavenReplicationDestinations;
			global.Source = LocalDatabase.TransactionalStorage.Id.ToString();

			foreach (var destination in global.Destinations)
			{
				destination.Database = LocalDatabase.Name;
			}

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key)
		{
			return Constants.Global.ReplicationDestinations;
		}
	}
}
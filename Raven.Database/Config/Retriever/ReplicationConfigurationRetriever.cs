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
	internal class ReplicationConfigurationRetriever : ConfigurationRetrieverBase<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>
	{
		protected override ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> ApplyGlobalDocumentToLocal(ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> global, ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> local, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			local.ClientConfiguration = local.ClientConfiguration ?? global.ClientConfiguration;

			foreach (var localDestination in local.Destinations)
			{
				localDestination.IsLocal = true;
				localDestination.IsGlobal = false;
			}

			foreach (var globalDestination in global.Destinations)
			{
				globalDestination.IsLocal = false;
				globalDestination.IsGlobal = true;

				var localDestinationExists = local.Destinations.Any(x => string.Equals(x.Url, globalDestination.Url, StringComparison.OrdinalIgnoreCase) && string.Equals(x.Database, localDatabase.Name, StringComparison.OrdinalIgnoreCase));
				if (localDestinationExists)
					continue;

				globalDestination.Database = localDatabase.Name;
				local.Destinations.Add(globalDestination);
			}

			return local;
		}

		protected override ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> ConvertGlobalDocumentToLocal(ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> global, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			global.Id = Constants.RavenReplicationDestinations;
			global.Source = localDatabase.TransactionalStorage.Id.ToString();

			foreach (var destination in global.Destinations)
			{
				destination.IsGlobal = true;
				destination.IsLocal = false;
				destination.Database = localDatabase.Name;
			}

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return Constants.Global.ReplicationDestinationsDocumentName;
		}
	}
}
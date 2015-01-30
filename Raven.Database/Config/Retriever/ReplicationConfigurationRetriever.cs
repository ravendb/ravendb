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
		public ReplicationConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
			: base(systemDatabase, localDatabase)
		{
		}

		protected override ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> ApplyGlobalDocumentToLocal(ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> global, ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> local)
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

				var localDestinationExists = local.Destinations.Any(x => string.Equals(x.Url, globalDestination.Url, StringComparison.OrdinalIgnoreCase) && string.Equals(x.Database, LocalDatabase.Name, StringComparison.OrdinalIgnoreCase));
				if (localDestinationExists)
					continue;

				globalDestination.Database = LocalDatabase.Name;
				local.Destinations.Add(globalDestination);
			}

			return local;
		}

		protected override ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> ConvertGlobalDocumentToLocal(ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> global)
		{
			global.Id = Constants.RavenReplicationDestinations;
			global.Source = LocalDatabase.TransactionalStorage.Id.ToString();

			foreach (var destination in global.Destinations)
			{
				destination.IsGlobal = true;
				destination.IsLocal = false;
				destination.Database = LocalDatabase.Name;
			}

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key)
		{
			return Constants.Global.ReplicationDestinationsDocumentName;
		}
	}
}
// -----------------------------------------------------------------------
//  <copyright file="ReplicationConfigurationRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;

namespace Raven.Database.Config.Retriever
{
	internal class ReplicationConfigurationRetriever
	{
		private readonly DocumentDatabase systemDatabase;

		private readonly DocumentDatabase localDatabase;

		public ReplicationConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			this.systemDatabase = systemDatabase;
			this.localDatabase = localDatabase;
		}

		public ConfigurationDocument<ReplicationDocument> GetReplicationDocument()
		{
			var global = systemDatabase.Documents.Get(Constants.RavenGlobalReplicationDestinations, null);
			var local = localDatabase.Documents.Get(Constants.RavenReplicationDestinations, null);

			if (global == null && local == null)
				return null;

			var configurationDocument = new ConfigurationDocument<ReplicationDocument>
			{
				GlobalExists = global != null,
				LocalExists = local != null
			};

			if (local != null)
			{
				configurationDocument.Etag = local.Etag;
				configurationDocument.Metadata = local.Metadata;
			}

			if (global == null)
			{
				configurationDocument.Document = local.DataAsJson.JsonDeserialization<ReplicationDocument>();
				return configurationDocument;
			}

			if (local == null)
			{
				configurationDocument.Document = ConvertGlobalReplicationDocumentToLocal(global, localDatabase);
				return configurationDocument;
			}

			configurationDocument.Document = ApplyGlobalReplicationDocumentToLocal(global, local, localDatabase);
			return configurationDocument;
		}

		private static ReplicationDocument ApplyGlobalReplicationDocumentToLocal(JsonDocument globalDocument, JsonDocument localDocument, DocumentDatabase localDatabase)
		{
			var local = localDocument.DataAsJson.JsonDeserialization<ReplicationDocument>();
			var global = globalDocument.DataAsJson.JsonDeserialization<ReplicationDocument>();

			local.ClientConfiguration = local.ClientConfiguration ?? global.ClientConfiguration;

			foreach (var globalDestination in global.Destinations)
			{
				var localDestination = local.Destinations.FirstOrDefault(x => string.Equals(x.Url, globalDestination.Url, StringComparison.OrdinalIgnoreCase) && string.Equals(x.Database, localDatabase.Name, StringComparison.OrdinalIgnoreCase));
				if (localDestination != null)
					continue;

				globalDestination.Database = localDatabase.Name;
				local.Destinations.Add(globalDestination);
			}

			return local;
		}

		private static ReplicationDocument ConvertGlobalReplicationDocumentToLocal(JsonDocument globalDocument, DocumentDatabase localDatabase)
		{
			var global = globalDocument.DataAsJson.JsonDeserialization<ReplicationDocument>();
			global.Id = Constants.RavenReplicationDestinations;
			global.Source = localDatabase.TransactionalStorage.Id.ToString();

			foreach (var destination in global.Destinations)
			{
				destination.Database = localDatabase.Name;
			}

			return global;
		}
	}
}
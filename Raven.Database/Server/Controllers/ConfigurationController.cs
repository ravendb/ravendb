// -----------------------------------------------------------------------
//  <copyright file="ConfigurationController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;
using System.Net.Http;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
	public class ConfigurationController : RavenDbApiController
	{
		[HttpGet]
		[RavenRoute("configuration/document/{*docId}")]
		[RavenRoute("configuration/{databaseName}/document/{*docId}")]
		public HttpResponseMessage ConfigurationGet(string docId)
		{
			if (Database == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			var configurationDocument = Database.ConfigurationRetriever.GetConfigurationDocumentAsJson(docId);
			if (configurationDocument == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(configurationDocument);
		}

		[HttpGet]
		[RavenRoute("configuration/replication")]
		[RavenRoute("configuration/{databaseName}/replication")]
		public HttpResponseMessage ReplicationConfigurationGet()
		{
			if (Database == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			var configurationDocument = Database.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument>(Constants.RavenReplicationDestinations);
			if (configurationDocument == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(configurationDocument.Document);
		}
	}
}
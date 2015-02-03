// -----------------------------------------------------------------------
//  <copyright file="ConfigurationController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Database.Config;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class ConfigurationController : RavenDbApiController
	{
		[HttpGet]
		[RavenRoute("configuration/document/{*docId}")]
        [RavenRoute("databases/{databaseName}/configuration/document/{*docId}")]
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
        [RavenRoute("configuration/settings")]
        [RavenRoute("databases/{databaseName}/configuration/settings")]
        public HttpResponseMessage ConfigurationSettingsGet()
        {
            if (Database == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            var configurationSettings = Database.ConfigurationRetriever.GetConfigurationSettings(GetQueryStringValues("key"));
            if (configurationSettings == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            return GetMessageWithObject(configurationSettings);
        }

        [HttpPut]
        [RavenRoute("configuration/global/settings")]
        public async Task<HttpResponseMessage> GlobalSettingsPut()
        {
            var etag = GetEtag();
            var globalSettingsDoc = await ReadJsonObjectAsync<GlobalSettingsDocument>();

            Protect(globalSettingsDoc);
            var json = RavenJObject.FromObject(globalSettingsDoc);

            var metadata = (etag != null) ? ReadInnerHeaders.FilterHeadersToObject() : new RavenJObject();
            var putResult = Database.Documents.Put(Constants.Global.GlobalSettingsDocumentKey, etag, json, metadata, null);

            return (etag == null) ? GetEmptyMessage() : GetMessageWithObject(putResult);
        }

        private void Protect(GlobalSettingsDocument settings)
        {
            if (settings.SecuredSettings == null)
            {
                settings.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var prop in settings.SecuredSettings.ToList())
            {
                if (prop.Value == null)
                    continue;
                var bytes = Encoding.UTF8.GetBytes(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                var protectedValue = ProtectedData.Protect(bytes, entrophy, DataProtectionScope.CurrentUser);
                settings.SecuredSettings[prop.Key] = Convert.ToBase64String(protectedValue);
            }
        }

		[HttpGet]
		[RavenRoute("configuration/replication")]
        [RavenRoute("databases/{databaseName}/configuration/replication")]
		public HttpResponseMessage ReplicationConfigurationGet()
		{
			if (Database == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			var configurationDocument = Database.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
			if (configurationDocument == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(configurationDocument.MergedDocument);
		}
	}
}
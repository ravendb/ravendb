// )-----------------------------------------------------------------------
//  <copyright file="ConfigurationController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Database.Config;
using Raven.Database.Config.Retriever;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class ConfigurationController : BaseDatabaseApiController
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
        [RavenRoute("configuration/global/settings")]
        public HttpResponseMessage ConfigurationGlobalSettingsGet()
        {
            var json = Database.Documents.Get(Constants.Global.GlobalSettingsDocumentKey, null);
            var globalSettings = json != null ? json.ToJson().JsonDeserialization<GlobalSettingsDocument>() : new GlobalSettingsDocument();
            GlobalSettingsDocumentProtector.Unprotect(globalSettings);
            return GetMessageWithObject(globalSettings, HttpStatusCode.OK, (json != null ) ? json.Etag : null);
        }

        [HttpGet]
        [RavenRoute("configuration/settings")]
        [RavenRoute("databases/{databaseName}/configuration/settings")]
        public HttpResponseMessage ConfigurationSettingsGet()
        {
            if (Database == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            if (CanExposeConfigOverTheWire() == false && Database.Configuration.Studio.AllowNonAdminUsersToSetupPeriodicExport == false)
            {
                return GetEmptyMessage(HttpStatusCode.Forbidden);
            }

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
            var globalSettingsDoc = await ReadJsonObjectAsync<GlobalSettingsDocument>().ConfigureAwait(false);

            GlobalSettingsDocumentProtector.Protect(globalSettingsDoc);
            var json = RavenJObject.FromObject(globalSettingsDoc);

            var metadata = (etag != null) ? ReadInnerHeaders.FilterHeadersToObject() : new RavenJObject();
            var putResult = Database.Documents.Put(Constants.Global.GlobalSettingsDocumentKey, etag, json, metadata, null);

            return GetMessageWithObject(putResult);
        }

        [HttpPut]
        [RavenRoute("configuration/periodicExportSettings")]
        [RavenRoute("databases/{databaseName}/configuration/periodicExportSettings")]
        public async Task<HttpResponseMessage> PeriodicExportSettingsPut()
        {
            if (Database == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            if (Database.Configuration.Studio.AllowNonAdminUsersToSetupPeriodicExport == false)
            {
                return GetMessageWithString("You can set periodic export settings using this endpoint only if AllowNonAdminUsersToSetupPeriodicExport config is enabled", HttpStatusCode.Unauthorized);
            }

            var docKey = Constants.Database.Prefix + DatabaseName;
            var databaseDocument = SystemDatabase.Documents.Get(docKey, null);


            var dbDoc = databaseDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
            DatabasesLandlord.Unprotect(dbDoc);

            dbDoc.SecuredSettings.Remove(Constants.PeriodicExport.AwsSecretKey);
            dbDoc.SecuredSettings.Remove(Constants.PeriodicExport.AzureStorageKey);

            dbDoc.Settings.Remove(Constants.PeriodicExport.AwsAccessKey);
            dbDoc.Settings.Remove(Constants.PeriodicExport.AzureStorageAccount);

            var newConfiguration = await ReadJsonAsync().ConfigureAwait(false);

            var awsAccessKey = newConfiguration.Value<string>(Constants.PeriodicExport.AwsAccessKey);
            var awsSecretKey = newConfiguration.Value<string>(Constants.PeriodicExport.AwsSecretKey);
            var azureStorageAccount = newConfiguration.Value<string>(Constants.PeriodicExport.AzureStorageAccount);
            var azureStorageKey = newConfiguration.Value<string>(Constants.PeriodicExport.AzureStorageKey);

            if (awsAccessKey != null)
                dbDoc.Settings[Constants.PeriodicExport.AwsAccessKey] = awsAccessKey;

            if (awsSecretKey != null)
                dbDoc.SecuredSettings[Constants.PeriodicExport.AwsSecretKey] = awsSecretKey;

            if (azureStorageAccount != null)
                dbDoc.Settings[Constants.PeriodicExport.AzureStorageAccount] = azureStorageAccount;

            if (azureStorageKey != null)
                dbDoc.Settings[Constants.PeriodicExport.AzureStorageKey] = azureStorageKey;

            DatabasesLandlord.Protect(dbDoc);

            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

            SystemDatabase.Documents.Put(docKey, databaseDocument.Etag, json, new RavenJObject(), null);

            return GetEmptyMessage();
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

        [HttpGet]
        [RavenRoute("configuration/versioning")]
        [RavenRoute("databases/{databaseName}/configuration/versioning")]
        public HttpResponseMessage VersioningConfigurationGet()
        {
            if (Database == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            int nextPageStart = 0;
            var systemDbPrefixes =
                ConfigurationRetriever.IsGlobalConfigurationEnabled ?
                    DatabasesLandlord.SystemDatabase.Documents.GetDocumentsWithIdStartingWith(Constants.Global.VersioningDocumentPrefix, null, null, 0, int.MaxValue, CancellationToken.None, ref nextPageStart) :
                    new RavenJArray();
            var localDbPrefixes = Database.Documents.GetDocumentsWithIdStartingWith(Constants.Versioning.RavenVersioningPrefix, null, null, 0, int.MaxValue, CancellationToken.None, ref nextPageStart);
            var systemDbIds = systemDbPrefixes.Select(x =>
                x
                    .Value<RavenJObject>("@metadata")
                    .Value<string>("@id")
                    .Replace(Constants.Global.VersioningDocumentPrefix, Constants.Versioning.RavenVersioningPrefix)).ToList();
            var localIds = localDbPrefixes.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id")).ToList();

            var idsToFetch = systemDbIds.Union(localIds);

            var configurations = idsToFetch.Select(id => Database.ConfigurationRetriever.GetConfigurationDocumentAsJson(id)).ToList();

            return GetMessageWithObject(configurations);
        }
    }
}

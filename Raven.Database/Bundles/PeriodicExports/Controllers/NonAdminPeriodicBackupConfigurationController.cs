// -----------------------------------------------------------------------
//  <copyright file="NonAdminPeriodicExportConfigurationController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.PeriodicExports.Controllers
{
    public class NonAdminPeriodicExportConfigurationController : BundlesApiController
    {
        public override string BundleName
        {
            get { return "PeriodicExport"; }
        }

        [HttpGet]
        [RavenRoute("periodicExport/settings")]
        [RavenRoute("databases/{databaseName}/periodicExport/settings")]
        public HttpResponseMessage SettingsGet()
        {
            if (Database == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            if (Database.Configuration.Studio.AllowNonAdminUsersToSetupPeriodicExport == false)
                return GetMessageWithString("You can get periodic export settings using this endpoint only if AllowNonAdminUsersToSetupPeriodicExport config is enabled", HttpStatusCode.Forbidden);

            var docKey = Constants.Database.Prefix + DatabaseName;
            var databaseDocument = DatabasesLandlord.SystemDatabase.Documents.Get(docKey, null);

            var dbDoc = databaseDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
            DatabasesLandlord.Unprotect(dbDoc);

            var response = new RavenJObject();

            string awsAccessKey;
            if (dbDoc.Settings.TryGetValue("Raven/AWSAccessKey", out awsAccessKey))
                response["Raven/AWSAccessKey"] = awsAccessKey;

            string awsSecretKey;
            if (dbDoc.SecuredSettings.TryGetValue("Raven/AWSSecretKey", out awsSecretKey))
                response["Raven/AWSSecretKey"] = awsSecretKey;

            string azureStorageAccount;
            if (dbDoc.Settings.TryGetValue("Raven/AzureStorageAccount", out azureStorageAccount))
                response["Raven/AzureStorageAccount"] = azureStorageAccount;

            string azureStorageKey;
            if (dbDoc.SecuredSettings.TryGetValue("Raven/AzureStorageKey", out azureStorageKey))
                response["Raven/AzureStorageKey"] = azureStorageKey;

            return GetMessageWithObject(response);
        }

        [HttpPut]
        [RavenRoute("periodicExport/settings")]
        [RavenRoute("databases/{databaseName}/periodicExport/settings")]
        public async Task<HttpResponseMessage> SettingsPut()
        {
            if (Database == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            if (Database.Configuration.Studio.AllowNonAdminUsersToSetupPeriodicExport == false)
            {
                return GetMessageWithString("You can set periodic export settings using this endpoint only if AllowNonAdminUsersToSetupPeriodicExport config is enabled", HttpStatusCode.Unauthorized);
            }

            var docKey = Constants.Database.Prefix + DatabaseName;
            var databaseDocument = DatabasesLandlord.SystemDatabase.Documents.Get(docKey, null);


            var dbDoc = databaseDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
            DatabasesLandlord.Unprotect(dbDoc);

            dbDoc.SecuredSettings.Remove("Raven/AWSSecretKey");
            dbDoc.SecuredSettings.Remove("Raven/AzureStorageKey");
            dbDoc.Settings.Remove("Raven/AWSAccessKey");
            dbDoc.Settings.Remove("Raven/AzureStorageAccount");

            var newConfiguration = await ReadJsonAsync().ConfigureAwait(false);

            var awsAccessKey = newConfiguration.Value<string>("Raven/AWSAccessKey");
            var awsSecretKey = newConfiguration.Value<string>("Raven/AWSSecretKey");
            var azureStorageAccount = newConfiguration.Value<string>("Raven/AzureStorageAccount");
            var azureStorageKey = newConfiguration.Value<string>("Raven/AzureStorageKey");

            if (awsAccessKey != null)
                dbDoc.Settings["Raven/AWSAccessKey"] = awsAccessKey;

            if (awsSecretKey != null)
                dbDoc.SecuredSettings["Raven/AWSSecretKey"] = awsSecretKey;

            if (azureStorageAccount != null)
                dbDoc.Settings["Raven/AzureStorageAccount"] = azureStorageAccount;

            if (azureStorageKey != null)
                dbDoc.Settings["Raven/AzureStorageKey"] = azureStorageKey;

            DatabasesLandlord.Protect(dbDoc);

            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

            DatabasesLandlord.SystemDatabase.Documents.Put(docKey, databaseDocument.Etag, json, new RavenJObject(), null);

            return GetEmptyMessage();
        }
    }
}

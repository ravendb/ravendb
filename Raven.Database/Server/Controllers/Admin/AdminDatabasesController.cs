using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers.Admin
{
    [RoutePrefix("")]
    public class AdminDatabasesController : BaseAdminController
    {
        private const string ToggleIndexing = "toggle-indexing";
        private const string ToggleRejectClients = "toggle-reject-clients";

        [HttpGet]
        [RavenRoute("admin/databases/{*id}")]
        public HttpResponseMessage DatabasesGet(string id)
        {
            if (IsSystemDatabase(id))
            {
                //fetch fake (empty) system database document
                var systemDatabaseDocument = new DatabaseDocument { Id = Constants.SystemDatabase };
                return GetMessageWithObject(systemDatabaseDocument);
            }

            var docKey = "Raven/Databases/" + id;
            var document = Database.Documents.Get(docKey, null);
            if (document == null)
                return GetMessageWithString("Database " + id + " wasn't found", HttpStatusCode.NotFound);

            var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
            dbDoc.Id = id;
            DatabasesLandlord.Unprotect(dbDoc);

            string activeBundles;
            if (dbDoc.Settings.TryGetValue(Constants.ActiveBundles, out activeBundles))
                dbDoc.Settings[Constants.ActiveBundles] = BundlesHelper.ProcessActiveBundles(activeBundles);

            return GetMessageWithObject(dbDoc, HttpStatusCode.OK, document.Etag);
        }

        [HttpPut]
        [RavenRoute("admin/databases/{*id}")]
        public async Task<HttpResponseMessage> DatabasesPut(string id)
        {
            if (IsSystemDatabase(id))
            {
                return GetMessageWithString("System database document cannot be changed!", HttpStatusCode.Forbidden);
            }

            MessageWithStatusCode databaseNameFormat = CheckNameFormat(id, Database.Configuration.DataDirectory);
            if (databaseNameFormat.Message != null)
            {
                return GetMessageWithString(databaseNameFormat.Message, databaseNameFormat.ErrorCode);
            }

            Etag etag = GetEtag();
            string error = CheckExistingDatabaseName(id, etag);
            if (error != null)
            {
                return GetMessageWithString(error, HttpStatusCode.BadRequest);
            }
            var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
            
            string bundles;			
            if (dbDoc.Settings.TryGetValue(Constants.ActiveBundles, out bundles) && bundles.Contains("Encryption"))
            {
                if (dbDoc.SecuredSettings == null || !dbDoc.SecuredSettings.ContainsKey(Constants.EncryptionKeySetting) ||
                    !dbDoc.SecuredSettings.ContainsKey(Constants.AlgorithmTypeSetting))
                {
                    return GetMessageWithString(string.Format("Failed to create '{0}' database, because of invalid encryption configuration.", id), HttpStatusCode.BadRequest);
                }
            }

            //TODO: check if paths in document are legal

            DatabasesLandlord.Protect(dbDoc);
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

            var metadata = (etag != null) ? ReadInnerHeaders.FilterHeadersToObject() : new RavenJObject();
            var docKey = Constants.Database.Prefix + id;
            var putResult = Database.Documents.Put(docKey, etag, json, metadata, null);

            return (etag == null) ? GetEmptyMessage() : GetMessageWithObject(putResult);
        }


        [HttpDelete]
        [RavenRoute("admin/databases/{*id}")]
        public HttpResponseMessage DatabasesDelete(string id)
        {
            bool result;
            var isHardDeleteNeeded = bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result;

            var message = DeleteDatabase(id, isHardDeleteNeeded);
            if (message.ErrorCode != HttpStatusCode.OK)
            {
                return GetMessageWithString(message.Message, message.ErrorCode);
            }

            return GetEmptyMessage();
        }

        [HttpDelete]
        [RavenRoute("admin/databases/batch-delete")]
        public HttpResponseMessage DatabasesBatchDelete()
        {
            string[] databasesToDelete = GetQueryStringValues("ids");
            if (databasesToDelete == null)
            {
                return GetMessageWithString("No databases to delete!", HttpStatusCode.BadRequest);
            }

            bool result;
            var isHardDeleteNeeded = bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result;
            var successfullyDeletedDatabases = new List<string>();

            databasesToDelete.ForEach(databaseId =>
            {
                var message = DeleteDatabase(databaseId, isHardDeleteNeeded);
                if (message.ErrorCode == HttpStatusCode.OK)
                {
                    successfullyDeletedDatabases.Add(databaseId);
                }

            });

            return GetMessageWithObject(successfullyDeletedDatabases.ToArray());
        }

        [HttpPost]
        [RavenRoute("admin/databases/{*id}")]
        public object OldToggleDisable(string id)
        {
            if (id.StartsWith(ToggleIndexing))			
            {
                string dbId = id.Substring(ToggleIndexing.Length + 1);
                var isSettingIndexingDisabledStr = GetQueryStringValue("isSettingIndexingDisabled");
                bool isSettingIndexingDisabled;
                if (!string.IsNullOrEmpty(isSettingIndexingDisabledStr) && bool.TryParse(isSettingIndexingDisabledStr, out isSettingIndexingDisabled))
                {
                    return ToggleIndexingDisable(dbId, isSettingIndexingDisabled);
                }
                return GetMessageWithString(string.Format("Failed to route call {0}", Request.RequestUri.OriginalString), HttpStatusCode.BadRequest);
            }
            if (id.StartsWith(ToggleRejectClients))
            {
                var dbId = id.Substring(ToggleRejectClients.Length + 1);
                var isRejectClientsEnabledStr = GetQueryStringValue("isRejectClientsEnabled");
                bool isRejectClientsEnabled;
                if (!string.IsNullOrEmpty(isRejectClientsEnabledStr) && bool.TryParse(isRejectClientsEnabledStr, out isRejectClientsEnabled))
                {
                    return DatabaseToggleRejectClientsEnabled(dbId, isRejectClientsEnabled);
                }
                return GetMessageWithString(string.Format("Failed to route call {0}", Request.RequestUri.OriginalString), HttpStatusCode.BadRequest);
            }
            var isSettingDisabledStr = GetQueryStringValue("isSettingDisabled");
            bool isSettingDisabled;
            if (!string.IsNullOrEmpty(isSettingDisabledStr) && bool.TryParse(isSettingDisabledStr, out isSettingDisabled))
                return ToggleDisable(id, isSettingDisabled);
            return GetMessageWithString(string.Format("Failed to route call {0}", Request.RequestUri.OriginalString), HttpStatusCode.BadRequest);
        }

        [HttpPost]
        [RavenRoute("admin/databases-toggle-disable")]
        public HttpResponseMessage ToggleDisable(string id, bool isSettingDisabled)
        {
            var message = ToggeleDatabaseDisabled(id, isSettingDisabled);
            if (message.ErrorCode != HttpStatusCode.OK)
            {
                return GetMessageWithString(message.Message, message.ErrorCode);
            }

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("admin/databases-toggle-indexing")]
        public HttpResponseMessage ToggleIndexingDisable(string id, bool isSettingIndexingDisabled)
        {
            var message = ToggeleDatabaseIndexingDisabled(id, isSettingIndexingDisabled);
            if (message.ErrorCode != HttpStatusCode.OK)
            {
                return GetMessageWithString(message.Message, message.ErrorCode);
            }

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("admin/databases-toggle-reject-clients")]
        public HttpResponseMessage DatabaseToggleRejectClientsEnabled(string id, bool isRejectClientsEnabled)
        {
            var message = ToggleRejectClientsEnabled(id, isRejectClientsEnabled);
            if (message.ErrorCode != HttpStatusCode.OK)
            {
                return GetMessageWithString(message.Message, message.ErrorCode);
            }

            return GetEmptyMessage();
        }
        
        [HttpPost]
        [RavenRoute("admin/databases/batch-toggle-disable")]
        public HttpResponseMessage DatabaseBatchToggleDisable(bool isSettingDisabled)
        {
            string[] databasesToToggle = GetQueryStringValues("ids");
            if (databasesToToggle == null)
            {
                return GetMessageWithString("No databases to toggle!", HttpStatusCode.BadRequest);
            }

            var successfullyToggledDatabases = new List<string>();

            databasesToToggle.ForEach(databaseId =>
            {
                var message = ToggeleDatabaseDisabled(databaseId, isSettingDisabled);
                if (message.ErrorCode == HttpStatusCode.OK)
                {
                    successfullyToggledDatabases.Add(databaseId);
                }

            });

            return GetMessageWithObject(successfullyToggledDatabases.ToArray());
        }

        private MessageWithStatusCode DeleteDatabase(string databaseId, bool isHardDeleteNeeded)
        {
            if (IsSystemDatabase(databaseId))
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.Forbidden, Message = "System Database document cannot be deleted" };

            //get configuration even if the database is disabled
            var configuration = DatabasesLandlord.CreateTenantConfiguration(databaseId, true);

            if (configuration == null)
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Database wasn't found" };

            var docKey = "Raven/Databases/" + databaseId;
            Database.Documents.Delete(docKey, null, null);

            if (isHardDeleteNeeded)
            {
                IOExtensions.DeleteDirectory(configuration.DataDirectory);
                if (configuration.IndexStoragePath != null)
                    IOExtensions.DeleteDirectory(configuration.IndexStoragePath);

                if (configuration.Storage.Esent.JournalsStoragePath != null)
                    IOExtensions.DeleteDirectory(configuration.Storage.Esent.JournalsStoragePath);

                if (configuration.Storage.Voron.JournalsStoragePath != null)
                    IOExtensions.DeleteDirectory(configuration.Storage.Voron.JournalsStoragePath);
            }

            return new MessageWithStatusCode();
        }

        private MessageWithStatusCode ToggeleDatabaseDisabled(string databaseId, bool isSettingDisabled)
        {
            if (IsSystemDatabase(databaseId))
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.Forbidden, Message = "System Database document cannot be disabled" };

            var docKey = "Raven/Databases/" + databaseId;
            var document = Database.Documents.Get(docKey, null);
            if (document == null)
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Database " + databaseId + " wasn't found" };

            var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
            if (dbDoc.Disabled == isSettingDisabled)
            {
                string state = isSettingDisabled ? "disabled" : "enabled";
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.BadRequest, Message = "Database " + databaseId + " is already " + state };
            }

            dbDoc.Disabled = !dbDoc.Disabled;
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");
            Database.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);

            return new MessageWithStatusCode();
        }

        private MessageWithStatusCode ToggeleDatabaseIndexingDisabled(string databaseId, bool isindexingDisabled)
        {
            if (IsSystemDatabase(databaseId))
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.Forbidden, Message = "System Database document indexing cannot be disabled" };

            var docKey = "Raven/Databases/" + databaseId;
            var document = Database.Documents.Get(docKey, null);
            if (document == null)
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Database " + databaseId + " wasn't found" };

            var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
            if (dbDoc.Settings.ContainsKey(Constants.IndexingDisabled))
            {
                bool indexDisabled;
                var success = bool.TryParse(dbDoc.Settings[Constants.IndexingDisabled], out indexDisabled);
                if (success && indexDisabled == isindexingDisabled)
                {
                    string state = isindexingDisabled ? "disabled" : "enabled";
                    return new MessageWithStatusCode {ErrorCode = HttpStatusCode.BadRequest, Message = "Database " + databaseId + "indexing is already " + state};
                }
            }
            dbDoc.Settings[Constants.IndexingDisabled] = isindexingDisabled.ToString();
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");
            Database.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);
            return new MessageWithStatusCode();
        }

        private MessageWithStatusCode ToggleRejectClientsEnabled(string databaseId, bool isRejectClientsEnabled)
        {
            if (IsSystemDatabase(databaseId))
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.Forbidden, Message = "System Database clients rejection can't change." };

            var docKey = "Raven/Databases/" + databaseId;
            var document = Database.Documents.Get(docKey, null);
            if (document == null)
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Database " + databaseId + " wasn't found" };

            var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
            if (dbDoc.Settings.ContainsKey(Constants.RejectClientsModeEnabled))
            {
                bool rejectClientsEnabled;
                var success = bool.TryParse(dbDoc.Settings[Constants.RejectClientsModeEnabled], out rejectClientsEnabled);
                if (success && rejectClientsEnabled == isRejectClientsEnabled)
                {
                    string state = rejectClientsEnabled ? "reject clients mode" : "accept clients mode";
                    return new MessageWithStatusCode {ErrorCode = HttpStatusCode.BadRequest, Message = "Database " + databaseId + "is already in " + state};
                }
            }
            dbDoc.Settings[Constants.RejectClientsModeEnabled] = isRejectClientsEnabled.ToString();
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");
            Database.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);
            return new MessageWithStatusCode();
        }

        private string CheckExistingDatabaseName(string id, Etag etag)
        {
            string errorMessage = null;
            var docKey = "Raven/Databases/" + id;
            var database = Database.Documents.Get(docKey, null);
            var isExistingDatabase = (database != null);

            if (isExistingDatabase && etag == null)
            {
                errorMessage = string.Format("Database with the name '{0}' already exists", id);
            }
            else if (!isExistingDatabase && etag != null)
            {
                errorMessage = string.Format("Database with the name '{0}' doesn't exist", id);
            }

            return errorMessage;
        }
    }
}

// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Json.Linq;

using Voron.Impl.Backup;


namespace Raven.Database.Server.RavenFS.Controllers
{
    public class AdminFileSystemController : BaseAdminController
    {
        [HttpPut]
        [Route("admin/fs/{*id}")]
        public async Task<HttpResponseMessage> FileSystemPut(string id, bool update = false)
        {
			
			MessageWithStatusCode fileSystemNameFormat = CheckNameFormat(id, Database.Configuration.FileSystem.DataDirectory);
			if (fileSystemNameFormat.Message != null)
			{
				return GetMessageWithString(fileSystemNameFormat.Message, fileSystemNameFormat.ErrorCode);
			}

            var docKey = "Raven/FileSystems/" + id;
           
            // There are 2 possible ways to call this put. We either want to update a filesystem configuration or we want to create a new one.            
            if (!update)
            {
                // As we are not updating, we should fail when the filesystem already exists.
                var existingFilesystem = Database.Documents.Get(docKey, null);
                if (existingFilesystem != null)                   
                    return GetEmptyMessage(HttpStatusCode.Conflict);
            }

			var fsDoc = await ReadJsonObjectAsync<DatabaseDocument>();
			FileSystemsLandlord.Protect(fsDoc);
			var json = RavenJObject.FromObject(fsDoc);
            json.Remove("Id");

            Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

		[HttpDelete]
		[Route("admin/fs/{*id}")]
		public HttpResponseMessage FileSystemDelete(string id)
		{
			bool result;
			var isHardDeleteNeeded = bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result;

			var message = DeleteFileSystem(id, isHardDeleteNeeded);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage();
		}

		[HttpDelete]
		[Route("admin/fs/batch-delete")]
		public HttpResponseMessage FileSystemBatchDelete()
		{
			string[] fileSystemsToDelete = GetQueryStringValues("ids");
			if (fileSystemsToDelete == null)
			{
				return GetMessageWithString("No file systems to delete!", HttpStatusCode.BadRequest);
			}

			bool result;
			var isHardDeleteNeeded = bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result;
			var successfullyDeletedDatabase = new List<string>();

			fileSystemsToDelete.ForEach(fileSystemId =>
			{
				var message = DeleteFileSystem(fileSystemId, isHardDeleteNeeded);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyDeletedDatabase.Add(fileSystemId);
				}

			});

			return GetMessageWithObject(successfullyDeletedDatabase.ToArray());
		}

		[HttpPost]
		[Route("admin/fs/{*id}")]
		public HttpResponseMessage FileSystemToggleDisable(string id, bool isSettingDisabled)
		{
			var message = ToggleFileSystemDisabled(id, isSettingDisabled);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage();
		}

		[HttpPost]
		[Route("admin/fs/batch-toggle-disable")]
		public HttpResponseMessage FileSystemBatchToggleDisable(bool isSettingDisabled)
		{
			string[] databasesToToggle = GetQueryStringValues("ids");
			if (databasesToToggle == null)
			{
				return GetMessageWithString("No file systems to toggle!", HttpStatusCode.BadRequest);
			}

			var successfullyToggledFileSystems = new List<string>();

			databasesToToggle.ForEach(fileSystemId =>
			{
				var message = ToggleFileSystemDisabled(fileSystemId, isSettingDisabled);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyToggledFileSystems.Add(fileSystemId);
				}

			});

			return GetMessageWithObject(successfullyToggledFileSystems.ToArray());
		}

		private MessageWithStatusCode DeleteFileSystem(string fileSystemId, bool isHardDeleteNeeded)
		{
			//get configuration even if the file system is disabled
			var configuration = FileSystemsLandlord.CreateTenantConfiguration(fileSystemId, true);

			if (configuration == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "File system wasn't found" };

			var docKey = "Raven/FileSystems/" + fileSystemId;
			Database.Documents.Delete(docKey, null, null);

			if (isHardDeleteNeeded)
			{
				IOExtensions.DeleteDirectory(configuration.FileSystem.DataDirectory);
				//TODO: find out which path should be deleted
				/*if (configuration.IndexStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.IndexStoragePath);*/
				/*if (configuration.JournalsStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.JournalsStoragePath);*/
			}

			return new MessageWithStatusCode();
		}

		private MessageWithStatusCode ToggleFileSystemDisabled(string fileSystemId, bool isSettingDisabled)
		{
			var docKey = "Raven/FileSystems/" + fileSystemId;
			var document = Database.Documents.Get(docKey, null);
			if (document == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "File system " + fileSystemId + " wasn't found" };

			var fsDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
			if (fsDoc.Disabled == isSettingDisabled)
			{
				string state = isSettingDisabled ? "disabled" : "enabled";
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.BadRequest, Message = "File system " + fileSystemId + " is already " + state };
			}

			FileSystemsLandlord.Unprotect(fsDoc);
			fsDoc.Disabled = !fsDoc.Disabled;
			var json = RavenJObject.FromObject(fsDoc);
			json.Remove("Id");
			Database.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);

			return new MessageWithStatusCode();
		}

        [HttpPost]
        [Route("admin/fs/backup")]
        [Route("fs/{fileSystemName}/admin/fs/backup")]
        public async Task<HttpResponseMessage> Backup()
        {
            var backupRequest = await ReadJsonObjectAsync<BackupRequest>();
            var incrementalString = InnerRequest.RequestUri.ParseQueryString()["incremental"];
            bool incrementalBackup;
            if (bool.TryParse(incrementalString, out incrementalBackup) == false)
                incrementalBackup = false;

            if (backupRequest.DatabaseDocument == null && Database.Name != null)
            {
                if (Database.Name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
                {
                    backupRequest.DatabaseDocument = new DatabaseDocument { Id = Constants.SystemDatabase };
                }
                else
                {
                    var jsonDocument = DatabasesLandlord.SystemDatabase.Documents.Get("Raven/Databases/" + Database.Name, null);
                    if (jsonDocument != null)
                    {
                        backupRequest.DatabaseDocument = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
                        DatabasesLandlord.Unprotect(backupRequest.DatabaseDocument);
                        backupRequest.DatabaseDocument.Id = Database.Name;
                    }
                }
            }

            Database.Maintenance.StartBackup(backupRequest.BackupLocation, incrementalBackup, backupRequest.DatabaseDocument);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

        [HttpPost]
        [Route("admin/fs/restore")]
        [Route("fs/{fileSystemName}/admin/fs/restore")]
        public async Task<HttpResponseMessage> Restore()
        {
            if (EnsureSystemDatabase() == false)
                return GetMessageWithString("Restore is only possible from the system database", HttpStatusCode.BadRequest);

            var restoreStatus = new RestoreStatus { Messages = new List<string>() };

            var restoreRequest = await ReadJsonObjectAsync<RestoreRequest>();

            DatabaseDocument databaseDocument = null;

            var databaseDocumentPath = Path.Combine(restoreRequest.BackupLocation, "Database.Document");
            if (File.Exists(databaseDocumentPath))
            {
                var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
                databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
            }

            var databaseName = !string.IsNullOrWhiteSpace(restoreRequest.DatabaseName) ? restoreRequest.DatabaseName
                                   : databaseDocument == null ? null : databaseDocument.Id;

            if (string.IsNullOrWhiteSpace(databaseName))
            {
                var errorMessage = (databaseDocument == null || String.IsNullOrWhiteSpace(databaseDocument.Id))
                                ? "Database.Document file is invalid - database name was not found and not supplied in the request (Id property is missing or null). This is probably a bug - should never happen."
                                : "A database name must be supplied if the restore location does not contain a valid Database.Document file";

                restoreStatus.Messages.Add(errorMessage);
                DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null, RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);

                return GetMessageWithString(errorMessage, HttpStatusCode.BadRequest);
            }

            if (databaseName == Constants.SystemDatabase)
                return GetMessageWithString("Cannot do an online restore for the <system> database", HttpStatusCode.BadRequest);

            var ravenConfiguration = new RavenConfiguration
            {
                DatabaseName = databaseName,
                IsTenantDatabase = true
            };

            if (databaseDocument != null)
            {
                foreach (var setting in databaseDocument.Settings)
                {
                    ravenConfiguration.Settings[setting.Key] = setting.Value;
                }
            }

            if (File.Exists(Path.Combine(restoreRequest.BackupLocation, BackupMethods.Filename)))
                ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
            else if (Directory.Exists(Path.Combine(restoreRequest.BackupLocation, "new")))
                ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

            ravenConfiguration.CustomizeValuesForTenant(databaseName);
            ravenConfiguration.Initialize();

            string documentDataDir;
            ravenConfiguration.DataDirectory = ResolveTenantDataDirectory(restoreRequest.DatabaseLocation, databaseName, out documentDataDir);
            restoreRequest.DatabaseLocation = ravenConfiguration.DataDirectory;
            DatabasesLandlord.SystemDatabase.Documents.Delete(RestoreStatus.RavenRestoreStatusDocumentKey, null, null);

            bool defrag;
            if (bool.TryParse(GetQueryStringValue("defrag"), out defrag))
                restoreRequest.Defrag = defrag;

            //TODO: add task to pending task list like in ImportDatabase
            Task.Factory.StartNew(() =>
            {
                MaintenanceActions.Restore(ravenConfiguration, restoreRequest,
                    msg =>
                    {
                        restoreStatus.Messages.Add(msg);
                        DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
                            RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
                    });

                if (databaseDocument == null)
                    return;

                databaseDocument.Settings[Constants.RavenDataDir] = documentDataDir;
                if (restoreRequest.IndexesLocation != null)
                    databaseDocument.Settings[Constants.RavenIndexPath] = restoreRequest.IndexesLocation;
                if (restoreRequest.JournalsLocation != null)
                    databaseDocument.Settings[Constants.RavenTxJournalPath] = restoreRequest.JournalsLocation;
                databaseDocument.Id = databaseName;
                DatabasesLandlord.Protect(databaseDocument);
                DatabasesLandlord.SystemDatabase.Documents.Put("Raven/Databases/" + databaseName, null, RavenJObject.FromObject(databaseDocument),
                    new RavenJObject(), null);

                restoreStatus.Messages.Add("The new database was created");
                DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
                    RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
            }, TaskCreationOptions.LongRunning);

            return GetEmptyMessage();
        }

        //TODO: extract to utils?
        private string ResolveTenantDataDirectory(string databaseLocation, string databaseName, out string documentDataDir)
        {
            if (Path.IsPathRooted(databaseLocation))
            {
                documentDataDir = databaseLocation;
                return databaseLocation;
            }

            var baseDataPath = Path.GetDirectoryName(DatabasesLandlord.SystemDatabase.Configuration.DataDirectory);
            if (baseDataPath == null)
                throw new InvalidOperationException("Could not find root data path");

            if (string.IsNullOrWhiteSpace(databaseLocation))
            {
                documentDataDir = Path.Combine("~\\Databases", databaseName);
                return Raven.Database.Extensions.IOExtensions.ToFullPath(documentDataDir, baseDataPath);
            }

            documentDataDir = databaseLocation;

            if (!documentDataDir.StartsWith("~/") && !documentDataDir.StartsWith(@"~\"))
            {
                documentDataDir = "~\\" + documentDataDir.TrimStart(new[] { '/', '\\' });
            }
            else if (documentDataDir.StartsWith("~/") || documentDataDir.StartsWith(@"~\"))
            {
                documentDataDir = "~\\" + documentDataDir.Substring(2);
            }

            return Raven.Database.Extensions.IOExtensions.ToFullPath(documentDataDir, baseDataPath);
        }
    }
}
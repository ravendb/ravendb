// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Database.Config;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Storage;
using Raven.Json.Linq;

using Voron.Impl.Backup;


namespace Raven.Database.Server.RavenFS.Controllers
{
    public class AdminFileSystemController : BaseAdminController
    {
        public string FilesystemName { get; private set; }

        protected override void InnerInitialization(HttpControllerContext controllerContext)
        {
            base.InnerInitialization(controllerContext);
            var values = controllerContext.Request.GetRouteData().Values;
            if (values.ContainsKey("MS_SubRoutes"))
            {
                var routeDatas = (IHttpRouteData[])controllerContext.Request.GetRouteData().Values["MS_SubRoutes"];
                var selectedData = routeDatas.FirstOrDefault(data => data.Values.ContainsKey("fileSystemName"));

                if (selectedData != null)
                    FilesystemName = selectedData.Values["fileSystemName"] as string;
                else
                    FilesystemName = null;
            }
            else
            {
                if (values.ContainsKey("fileSystemName"))
                    FilesystemName = values["fileSystemName"] as string;
                else
                    FilesystemName = null;
            }
        }

        public RavenFileSystem FileSystem
        {
            get
            {
                var filesystem = FileSystemsLandlord.GetFileSystemInternal(FilesystemName);
                if (filesystem == null)
                {
                    throw new InvalidOperationException("Could not find a filesystem named: " + FilesystemName);
                }

                return filesystem.Result;
            }
        }

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

			var fsDoc = document.DataAsJson.JsonDeserialization<FileSystemDocument>();
			if (fsDoc.Disabled == isSettingDisabled)
			{
				string state = isSettingDisabled ? "disabled" : "enabled";
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.BadRequest, Message = "File system " + fileSystemId + " is already " + state };
			}

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
            var backupRequest = await ReadJsonObjectAsync<FilesystemBackupRequest>();
            var incrementalString = InnerRequest.RequestUri.ParseQueryString()["incremental"];
            bool incrementalBackup;
            if (bool.TryParse(incrementalString, out incrementalBackup) == false)
                incrementalBackup = false;

            if (backupRequest.FileSystemDocument == null && FileSystem.Name != null)
            {
                var jsonDocument = DatabasesLandlord.SystemDatabase.Documents.Get("Raven/Filesystems/" + FileSystem.Name, null);
                if (jsonDocument != null)
                {
                    backupRequest.FileSystemDocument = jsonDocument.DataAsJson.JsonDeserialization<FileSystemDocument>();
                    backupRequest.FileSystemDocument.Id = FileSystem.Name;
                }
            }

            var transactionalStorage = FileSystem.Storage;
            var filesystemDocument = backupRequest.FileSystemDocument;
            var backupDestinationDirectory = backupRequest.BackupLocation;

            var document = Database.Documents.Get(BackupStatus.RavenFilesystemBackupStatusDocumentKey(FilesystemName), null);
            if (document != null)
            {
                var backupStatus = document.DataAsJson.JsonDeserialization<BackupStatus>();
                if (backupStatus.IsRunning)
                {
                    throw new InvalidOperationException("Backup is already running");
                }
            }

            //TODO: verify those properies (for Filesystem)
            bool enableIncrementalBackup;
            if (incrementalBackup &&
                transactionalStorage is Storage.Esent.TransactionalStorage &&
                (bool.TryParse(Database.Configuration.Settings["Raven/Esent/CircularLog"], out enableIncrementalBackup) == false || enableIncrementalBackup))
            {
                throw new InvalidOperationException("In order to run incremental backups using Esent you must have circular logging disabled");
            }

            if (incrementalBackup &&
                transactionalStorage is Storage.Voron.TransactionalStorage &&
                Database.Configuration.Storage.Voron.AllowIncrementalBackups == false)
            {
                throw new InvalidOperationException("In order to run incremental backups using Voron you must have the appropriate setting key (Raven/Voron/AllowIncrementalBackups) set to true");
            }

            Database.Documents.Put(BackupStatus.RavenFilesystemBackupStatusDocumentKey(FilesystemName), null, RavenJObject.FromObject(new BackupStatus
            {
                Started = SystemTime.UtcNow,
                IsRunning = true,
            }), new RavenJObject(), null);

            if (filesystemDocument.Settings.ContainsKey("Raven/StorageTypeName") == false)
                filesystemDocument.Settings["Raven/StorageTypeName"] = transactionalStorage.FriendlyName ?? transactionalStorage.GetType().AssemblyQualifiedName;

            transactionalStorage.StartBackupOperation(DatabasesLandlord.SystemDatabase, FileSystem, backupDestinationDirectory, incrementalBackup, filesystemDocument);

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

            var restoreRequest = await ReadJsonObjectAsync<FilesystemRestoreRequest>();

            FileSystemDocument filesystemDocument = null;

            var fileSystemDocumentPath = Path.Combine(restoreRequest.BackupLocation, BackupMethods.FilesystemDocumentFilename);

            if (!File.Exists(fileSystemDocumentPath))
            {
                throw new InvalidOperationException("Cannot restore when the Filesystem.Document file is missing in the backup folder: " + restoreRequest.BackupLocation);
            }

            var filesystemDocumentText = File.ReadAllText(fileSystemDocumentPath);
            filesystemDocument = RavenJObject.Parse(filesystemDocumentText).JsonDeserialization<FileSystemDocument>();

            var filesystemName = !string.IsNullOrWhiteSpace(restoreRequest.FilesystemName) ? restoreRequest.FilesystemName
                                   : filesystemDocument == null ? null : filesystemDocument.Id;

            if (string.IsNullOrWhiteSpace(filesystemName))
            {
                var errorMessage = (filesystemDocument == null || String.IsNullOrWhiteSpace(filesystemDocument.Id))
                                ? BackupMethods.FilesystemDocumentFilename +  " file is invalid - filesystem name was not found and not supplied in the request (Id property is missing or null). This is probably a bug - should never happen."
                                : "A filesystem name must be supplied if the restore location does not contain a valid " + BackupMethods.FilesystemDocumentFilename + " file";

                restoreStatus.Messages.Add(errorMessage);
                DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null, RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);

                return GetMessageWithString(errorMessage, HttpStatusCode.BadRequest);
            }

            var ravenConfiguration = new RavenConfiguration
            {
                DatabaseName = filesystemName,
                IsTenantDatabase = true
            };

            if (filesystemDocument != null)
            {
                foreach (var setting in filesystemDocument.Settings)
                {
                    ravenConfiguration.Settings[setting.Key] = setting.Value;
                }
            }

            if (File.Exists(Path.Combine(restoreRequest.BackupLocation, BackupMethods.Filename)))
                ravenConfiguration.FileSystem.DefaultStorageTypeName = InMemoryRavenConfiguration.VoronTypeName;
            else if (Directory.Exists(Path.Combine(restoreRequest.BackupLocation, "new")))
                ravenConfiguration.FileSystem.DefaultStorageTypeName = InMemoryRavenConfiguration.EsentTypeName;
            
            ravenConfiguration.CustomizeValuesForTenant(filesystemName);
            ravenConfiguration.Initialize();

            string documentDataDir;
            ravenConfiguration.FileSystem.DataDirectory = ResolveTenantDataDirectory(restoreRequest.FilesystemLocation, filesystemName, out documentDataDir);
            restoreRequest.FilesystemLocation = ravenConfiguration.FileSystem.DataDirectory;
            
            DatabasesLandlord.SystemDatabase.Documents.Delete(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null, null);

            bool defrag;
            if (bool.TryParse(GetQueryStringValue("defrag"), out defrag))
                restoreRequest.Defrag = defrag;

            //TODO: add task to pending task list like in ImportDatabase
            Task.Factory.StartNew(() =>
            {
                if (!string.IsNullOrWhiteSpace(restoreRequest.FilesystemLocation))
                {
                    ravenConfiguration.FileSystem.DataDirectory = restoreRequest.FilesystemLocation;
                }

                using (var transactionalStorage = RavenFileSystem.CreateTransactionalStorage(ravenConfiguration))
                {
                    transactionalStorage.Restore(restoreRequest, msg =>
                    {
                        restoreStatus.Messages.Add(msg);
                        DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null,
                            RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
                    });
                }

                if (filesystemDocument == null)
                    return;

                filesystemDocument.Settings["Raven/FileSystem/DataDir"] = documentDataDir;
                if (restoreRequest.IndexesLocation != null)
                    filesystemDocument.Settings[Constants.RavenIndexPath] = restoreRequest.IndexesLocation;
                if (restoreRequest.JournalsLocation != null)
                    filesystemDocument.Settings[Constants.RavenTxJournalPath] = restoreRequest.JournalsLocation;
                filesystemDocument.Id = filesystemName;
                DatabasesLandlord.SystemDatabase.Documents.Put("Raven/FileSystems/" + filesystemName, null, RavenJObject.FromObject(filesystemDocument),
                    new RavenJObject(), null);

                restoreStatus.Messages.Add("The new filesystem was created");
                DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null,
                    RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
            }, TaskCreationOptions.LongRunning);

            return GetEmptyMessage();
        }

        private string ResolveTenantDataDirectory(string filesystemLocation, string filesystemName, out string documentDataDir)
        {
            if (Path.IsPathRooted(filesystemLocation))
            {
                documentDataDir = filesystemLocation;
                return filesystemLocation;
            }

            var baseDataPath = Path.GetDirectoryName(DatabasesLandlord.SystemDatabase.Configuration.DataDirectory);
            if (baseDataPath == null)
                throw new InvalidOperationException("Could not find root data path");

            if (string.IsNullOrWhiteSpace(filesystemLocation))
            {
                documentDataDir = Path.Combine("~\\Filesystems", filesystemName);
                return Raven.Database.Extensions.IOExtensions.ToFullPath(documentDataDir, baseDataPath);
            }

            documentDataDir = filesystemLocation;

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
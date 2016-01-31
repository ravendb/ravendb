// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;


namespace Raven.Database.FileSystem.Controllers
{
    public class AdminFileSystemController : BaseAdminController
    {
        private static readonly char[] existingDriveLetters = DriveInfo.GetDrives().Select(x => x.Name.ToLower()[0]).ToArray();


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
                var filesystem = AsyncHelpers.RunSync(() => FileSystemsLandlord.GetFileSystemInternalAsync(FilesystemName));
                if (filesystem == null)
                {
                    throw new InvalidOperationException("Could not find a filesystem named: " + FilesystemName);
                }

                return filesystem;
            }
        }

        [HttpPut]
        [RavenRoute("admin/fs/{*id}")]
        public async Task<HttpResponseMessage> FileSystemPut(string id, bool update = false)
        {
            
            MessageWithStatusCode fileSystemNameFormat = CheckNameFormat(id, Database.Configuration.FileSystem.DataDirectory);
            if (fileSystemNameFormat.Message != null)
            {
                return GetMessageWithObject(new
                {
                    Error = fileSystemNameFormat.Message
                }, fileSystemNameFormat.ErrorCode);
            }

            if (Authentication.IsLicensedForRavenFs == false)
            {
                return GetMessageWithObject(new
                {
                    Error = "Your license does not allow the use of RavenFS!"
                }, HttpStatusCode.BadRequest);
            }

            var docKey = Constants.FileSystem.Prefix + id;
           
            // There are 2 possible ways to call this put. We either want to update a filesystem configuration or we want to create a new one.            
            if (!update)
            {
                // As we are not updating, we should fail when the filesystem already exists.
                var existingFilesystem = Database.Documents.Get(docKey, null);
                if (existingFilesystem != null)                   
                    return GetEmptyMessage(HttpStatusCode.Conflict);
            }

            var fsDoc = await ReadJsonObjectAsync<FileSystemDocument>();
            EnsureFileSystemHasRequiredSettings(id, fsDoc);

            string bundles;
            if (fsDoc.Settings.TryGetValue(Constants.ActiveBundles, out bundles) && bundles.IndexOf("Encryption", StringComparison.OrdinalIgnoreCase) != -1)
            {
                if (fsDoc.SecuredSettings == null || !fsDoc.SecuredSettings.ContainsKey(Constants.EncryptionKeySetting) ||
                    !fsDoc.SecuredSettings.ContainsKey(Constants.AlgorithmTypeSetting))
                {
                    return GetMessageWithString(string.Format("Failed to create '{0}' file system, because of invalid encryption configuration.", id), HttpStatusCode.BadRequest);
                }
            }

            FileSystemsLandlord.Protect(fsDoc);
            var json = RavenJObject.FromObject(fsDoc);
            json.Remove("Id");

            Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

        private void EnsureFileSystemHasRequiredSettings(string id, FileSystemDocument fsDoc)
        {
            if (!fsDoc.Settings.ContainsKey(Constants.FileSystem.DataDirectory))
                fsDoc.Settings[Constants.FileSystem.DataDirectory] = "~/FileSystems/" + id;
        }

        [HttpDelete]
        [RavenRoute("admin/fs/{*id}")]
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
        [RavenRoute("admin/fs/batch-delete")]
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
        [RavenRoute("admin/fs/{*id}")]
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
        [RavenRoute("admin/fs/batch-toggle-disable")]
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

            var docKey = Constants.FileSystem.Prefix + fileSystemId;
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
        [RavenRoute("fs/{fileSystemName}/admin/reset-index")]
        public HttpResponseMessage ResetIndex ()
        {
            this.FileSystem.Search.ForceIndexReset();
            return GetEmptyMessage(HttpStatusCode.OK);
        }

        [HttpPost]
        [RavenRoute("fs/admin/backup")]
        [RavenRoute("fs/{fileSystemName}/admin/backup")]
        public async Task<HttpResponseMessage> Backup()
        {
            var backupRequest = await ReadJsonObjectAsync<FilesystemBackupRequest>();
            var incrementalString = InnerRequest.RequestUri.ParseQueryString()["incremental"];
            bool incrementalBackup;
            if (bool.TryParse(incrementalString, out incrementalBackup) == false)
                incrementalBackup = false;


            if (backupRequest.FileSystemDocument == null && FileSystem.Name != null)
            {
                var jsonDocument = DatabasesLandlord.SystemDatabase.Documents.Get("Raven/FileSystems/" + FileSystem.Name, null);
                if (jsonDocument != null)
                {
                    backupRequest.FileSystemDocument = jsonDocument.DataAsJson.JsonDeserialization<FileSystemDocument>();
                    FileSystemsLandlord.Unprotect(backupRequest.FileSystemDocument);
                    backupRequest.FileSystemDocument.Id = FileSystem.Name;
                }
            }

            var transactionalStorage = FileSystem.Storage;
            var filesystemDocument = backupRequest.FileSystemDocument;
            var backupDestinationDirectory = backupRequest.BackupLocation;

            RavenJObject document = null;
            try
            {
                FileSystem.Storage.Batch(accessor => document = accessor.GetConfig(BackupStatus.RavenBackupStatusDocumentKey));
            }
            catch (FileNotFoundException)
            {
                // ok, there isn't another backup in progress
            }
            

            if (document != null)
            {
                var backupStatus = document.JsonDeserialization<BackupStatus>();
                if (backupStatus.IsRunning)
                {
                    throw new InvalidOperationException("Backup is already running");
                }
            }

            HttpResponseMessage message;
            if (!HasPermissions(backupDestinationDirectory, out message))
                return message;

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

            FileSystem.Storage.Batch(accessor => accessor.SetConfig(BackupStatus.RavenBackupStatusDocumentKey, RavenJObject.FromObject(new BackupStatus
            {
                Started = SystemTime.UtcNow,
                IsRunning = true,
            })));

            if (filesystemDocument.Settings.ContainsKey(Constants.FileSystem.Storage) == false)
                filesystemDocument.Settings[Constants.FileSystem.Storage] = transactionalStorage.FriendlyName.ToLower() ?? transactionalStorage.GetType().AssemblyQualifiedName;

            transactionalStorage.StartBackupOperation(DatabasesLandlord.SystemDatabase, FileSystem, backupDestinationDirectory, incrementalBackup, filesystemDocument);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

        [HttpPost]
        [RavenRoute("admin/fs/compact")]
        public HttpResponseMessage Compact()
        {
            var fs = InnerRequest.RequestUri.ParseQueryString()["filesystem"];
            if (string.IsNullOrWhiteSpace(fs))
                return GetMessageWithString("Compact request requires a valid filesystem parameter", HttpStatusCode.BadRequest);

            var configuration = FileSystemsLandlord.CreateTenantConfiguration(fs);
            if (configuration == null)
                return GetMessageWithString("No filesystem named: " + fs, HttpStatusCode.NotFound);

            var task = Task.Factory.StartNew(() =>
            {
                var compactStatus = new CompactStatus { State = CompactStatusState.Running, Messages = new List<string>() };
                DatabasesLandlord.SystemDatabase.Documents.Delete(CompactStatus.RavenFilesystemCompactStatusDocumentKey(fs), null, null);
                try
                {
                    // as we perform compact async we don't catch exceptions here - they will be propagated to operation
                    var targetFs = AsyncHelpers.RunSync(() => FileSystemsLandlord.GetFileSystemInternalAsync(fs));
                    FileSystemsLandlord.Lock(fs, () => targetFs.Storage.Compact(configuration, msg =>
                    {
                        bool skipProgressReport = false;
                        bool isProgressReport = false;
                        if (IsUpdateMessage(msg))
                        {
                            isProgressReport = true;
                            var now = SystemTime.UtcNow;
                            compactStatus.LastProgressMessageTime = compactStatus.LastProgressMessageTime ?? DateTime.MinValue;
                            var timeFromLastUpdate = (now - compactStatus.LastProgressMessageTime.Value);
                            if (timeFromLastUpdate >= ReportProgressInterval)
                            {
                                compactStatus.LastProgressMessageTime = now;
                                compactStatus.LastProgressMessage = msg;
                            }
                            else skipProgressReport = true;

                        }
                        if (!skipProgressReport)
                        {
                            if (!isProgressReport) compactStatus.Messages.Add(msg);
                            DatabasesLandlord.SystemDatabase.Documents.Put(CompactStatus.RavenFilesystemCompactStatusDocumentKey(fs), null,
                                RavenJObject.FromObject(compactStatus), new RavenJObject(), null);
                        }
                    }));
                    compactStatus.State = CompactStatusState.Completed;
                    compactStatus.Messages.Add("File system compaction completed.");
                    DatabasesLandlord.SystemDatabase.Documents.Put(CompactStatus.RavenFilesystemCompactStatusDocumentKey(fs), null,
                        RavenJObject.FromObject(compactStatus), new RavenJObject(), null);
                }
                catch (Exception e)
                {
                    compactStatus.Messages.Add("Unable to compact file system " + e.Message);
                    compactStatus.State = CompactStatusState.Faulted;
                    DatabasesLandlord.SystemDatabase.Documents.Put(CompactStatus.RavenFilesystemCompactStatusDocumentKey(fs), null,
                                                                       RavenJObject.FromObject(compactStatus), new RavenJObject(), null);
                    throw;
                }
                return GetEmptyMessage();
            });

            long id;
            Database.Tasks.AddTask(task, new TaskBasedOperationState(task), new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.CompactFilesystem,
                Payload = "Compact filesystem " + fs,
            }, out id);

            return GetMessageWithObject(new
            {
                OperationId = id
            });
        }

        private static bool IsUpdateMessage(string msg)
        {
            if (String.IsNullOrEmpty(msg)) return false;
            //Here we check if we the message is in voron update format
            if (msg.StartsWith(VoronProgressString)) return true;
            //Here we check if we the messafe is in esent update format
            if (msg.Length > 42 && String.Compare(msg, 32, EsentProgressString, 0, 10) == 0) return true;
            return false;
        }
        private static TimeSpan ReportProgressInterval = TimeSpan.FromSeconds(1);
        private static string EsentProgressString = "JET_SNPROG";
        private static string VoronProgressString = "Copied";

        private bool IsValidPath(string path,out HttpResponseMessage message)
        {
            message = null;
            var localPath = new DirectoryInfo(path.ToFullPath());
            if (localPath.Exists)
                return true;

            message = GetMessageWithObject(new
            {
                Message = string.Format("Non-existing path : {0}", path)
            }, HttpStatusCode.BadRequest);
            return false;
        }

        private bool IsOnValidDrive(string path, out HttpResponseMessage message)
        {
            message = null;
            var fullPath = path.ToFullPath().ToLower();
            if (existingDriveLetters.Contains(fullPath[0]))
                return true;

            message = GetMessageWithObject(new
            {
                Message = string.Format("Non-existing path : {0}", path)
            }, HttpStatusCode.BadRequest);
            return false;
        }

        [HttpPost]
        [RavenRoute("admin/fs/restore")]
        [RavenRoute("fs/{fileSystemName}/admin/restore")]
        public async Task<HttpResponseMessage> Restore()
        {
            HttpResponseMessage message = null;
            if (EnsureSystemDatabase() == false)
                return GetMessageWithString("Restore is only possible from the system database", HttpStatusCode.BadRequest);

            var restoreStatus = new RestoreStatus { State = RestoreStatusState.Running, Messages = new List<string>() };

            var restoreRequest = await ReadJsonObjectAsync<FilesystemRestoreRequest>();

            if (!HasPermissions(restoreRequest.BackupLocation, out message))
                return message;

            var fileSystemDocumentPath = FindFilesystemDocument(restoreRequest.BackupLocation);

            if (!File.Exists(fileSystemDocumentPath))
            {
                throw new InvalidOperationException("Cannot restore when the Filesystem.Document file is missing in the backup folder: " + restoreRequest.BackupLocation);
            }

            var filesystemDocumentText = File.ReadAllText(fileSystemDocumentPath);
            var filesystemDocument = RavenJObject.Parse(filesystemDocumentText).JsonDeserialization<FileSystemDocument>();

            var filesystemName = !string.IsNullOrWhiteSpace(restoreRequest.FilesystemName) ? restoreRequest.FilesystemName
                                   : filesystemDocument == null ? null : filesystemDocument.Id;

            if (string.IsNullOrWhiteSpace(filesystemName))
            {
                var errorMessage = (filesystemDocument == null || String.IsNullOrWhiteSpace(filesystemDocument.Id))
                                ? Constants.FilesystemDocumentFilename +  " file is invalid - filesystem name was not found and not supplied in the request (Id property is missing or null). This is probably a bug - should never happen."
                                : "A filesystem name must be supplied if the restore location does not contain a valid " + Constants.FilesystemDocumentFilename + " file";

                restoreStatus.Messages.Add(errorMessage);
                DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null, RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);

                return GetMessageWithString(errorMessage, HttpStatusCode.BadRequest);
            }

            var ravenConfiguration = new RavenConfiguration
            {
                FileSystemName = filesystemName,
            };

            if (restoreRequest.IndexesLocation != null && 
                !IsValidPath(restoreRequest.IndexesLocation, out message))
                return message;
            if (restoreRequest.JournalsLocation != null &&
                !IsValidPath(restoreRequest.JournalsLocation, out message))
                return message;

            if (filesystemDocument != null)
            {
                //no need to check for existence of specified data and index folders here,
                //since those would be simply created after restore
                //we need to check only that the drive letter exists, so the folder can actually be created

                string dataLocation;
                if (filesystemDocument.Settings.TryGetValue(Constants.FileSystem.DataDirectory, out dataLocation))
                {
                    dataLocation = filesystemDocument.Settings[Constants.FileSystem.DataDirectory];
                    if (!IsOnValidDrive(dataLocation, out message))
                        return message;

                    if (!HasPermissions(dataLocation, out message))
                        return message;
                }

                string indexesLocation;
                if (filesystemDocument.Settings.TryGetValue(Constants.FileSystem.IndexStorageDirectory, out indexesLocation))
                {
                    indexesLocation = filesystemDocument.Settings[Constants.FileSystem.IndexStorageDirectory];
                    if (!IsOnValidDrive(indexesLocation, out message))
                        return message;

                    if (!HasPermissions(indexesLocation, out message))
                        return message;
                }
                foreach (var setting in filesystemDocument.Settings)
                {
                    ravenConfiguration.Settings[setting.Key] = setting.Value;
                }
            }

            ravenConfiguration.FileSystem.DefaultStorageTypeName = Directory.Exists(Path.Combine(restoreRequest.BackupLocation, "new")) ? 
                InMemoryRavenConfiguration.EsentTypeName : InMemoryRavenConfiguration.VoronTypeName;

            ravenConfiguration.CustomizeValuesForFileSystemTenant(filesystemName);
            ravenConfiguration.Initialize();

            string documentDataDir;
            ravenConfiguration.FileSystem.DataDirectory = ResolveTenantDataDirectory(restoreRequest.FilesystemLocation, filesystemName, out documentDataDir);
            restoreRequest.FilesystemLocation = ravenConfiguration.FileSystem.DataDirectory;

            string anotherRestoreResourceName;
            if (IsAnotherRestoreInProgress(out anotherRestoreResourceName))
            {
                if (restoreRequest.RestoreStartTimeout.HasValue)
                {
                    try
                    {
                        using (var cts = new CancellationTokenSource())
                        {
                            cts.CancelAfter(TimeSpan.FromSeconds(restoreRequest.RestoreStartTimeout.Value));
                            var token = cts.Token;
                            do
                            {
                                await Task.Delay(500, token);
                            }
                            while (IsAnotherRestoreInProgress(out anotherRestoreResourceName));
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return GetMessageWithString(string.Format("Another restore is still in progress (resource name = {0}). Waited {1} seconds for other restore to complete.", anotherRestoreResourceName, restoreRequest.RestoreStartTimeout.Value), HttpStatusCode.ServiceUnavailable);
                    }
                }
                else
                {
                    return GetMessageWithString(string.Format("Another restore is in progress (resource name = {0})", anotherRestoreResourceName), HttpStatusCode.ServiceUnavailable);
                }
            }
            Database.Documents.Put(RestoreInProgress.RavenRestoreInProgressDocumentKey, null, RavenJObject.FromObject(new RestoreInProgress
            {
                Resource = filesystemName
            }), new RavenJObject(), null);

            DatabasesLandlord.SystemDatabase.Documents.Delete(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null, null);
            
            bool defrag;
            if (bool.TryParse(GetQueryStringValue("defrag"), out defrag))
                restoreRequest.Defrag = defrag;

            var task = Task.Factory.StartNew(() =>
            {
                try
                {
                    if (!string.IsNullOrWhiteSpace(restoreRequest.FilesystemLocation))
                        ravenConfiguration.FileSystem.DataDirectory = restoreRequest.FilesystemLocation;

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

                    filesystemDocument.Settings[Constants.FileSystem.DataDirectory] = documentDataDir;

                    if (restoreRequest.IndexesLocation != null)
                        filesystemDocument.Settings[Constants.RavenIndexPath] = restoreRequest.IndexesLocation;
                    if (restoreRequest.JournalsLocation != null)
                        filesystemDocument.Settings[Constants.RavenTxJournalPath] = restoreRequest.JournalsLocation;
                    filesystemDocument.Id = filesystemName;

                    FileSystemsLandlord.Protect(filesystemDocument);

                    DatabasesLandlord.SystemDatabase.Documents.Put(Constants.FileSystem.Prefix + filesystemName, null, RavenJObject.FromObject(filesystemDocument), new RavenJObject(), null);

                    restoreStatus.State = RestoreStatusState.Completed;
                    restoreStatus.Messages.Add("The new filesystem was created");
                    DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null, RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
                }
                catch (Exception e)
                {
                    restoreStatus.State = RestoreStatusState.Faulted;
                    restoreStatus.Messages.Add("Unable to restore filesystem " + e.Message);
                    DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null, RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
                    throw;
                }
                finally
                {
                    Database.Documents.Delete(RestoreInProgress.RavenRestoreInProgressDocumentKey, null, null);
                }
            }, TaskCreationOptions.LongRunning);

            long id;
            Database.Tasks.AddTask(task, new TaskBasedOperationState(task), new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.RestoreFilesystem,
                Payload = "Restoring filesystem " + filesystemName + " from " + restoreRequest.BackupLocation
            }, out id);

            return GetMessageWithObject(new
            {
                OperationId = id
            });
        }	   

        private string FindFilesystemDocument(string rootBackupPath)
        {
            // try to find newest filesystem document in incremental backups first - to have the most recent version (if available)
            var backupPath = Directory.GetDirectories(rootBackupPath, "Inc*")
                                       .OrderByDescending(dir => dir)
                                       .Select(dir => Path.Combine(dir, Constants.FilesystemDocumentFilename))
                                       .FirstOrDefault();

            return backupPath ?? Path.Combine(rootBackupPath, Constants.FilesystemDocumentFilename);
        }

        private string ResolveTenantDataDirectory(string filesystemLocation, string filesystemName, out string documentDataDir)
        {
            if (Path.IsPathRooted(filesystemLocation))
            {
                documentDataDir = filesystemLocation;
                return documentDataDir;
            }

            var baseDataPath = Path.GetDirectoryName(this.FileSystemsLandlord.SystemConfiguration.FileSystem.DataDirectory);
            if (baseDataPath == null)
                throw new InvalidOperationException("Could not find root data path");

            if (string.IsNullOrWhiteSpace(filesystemLocation))
            {
                documentDataDir = Path.Combine("~\\FileSystems", filesystemName);
                return IOExtensions.ToFullPath(documentDataDir, baseDataPath);
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

            return IOExtensions.ToFullPath(documentDataDir, baseDataPath);
        }
    }
}

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
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Database.FileSystem.Synchronization;

namespace Raven.Database.FileSystem.Controllers
{
    public class AdminFileSystemController : BaseAdminFileSystemApiController
    {
        private static readonly char[] ExistingDriveLetters = DriveInfo.GetDrives().Select(x => x.Name.ToLower()[0]).ToArray();

        [HttpPut]
        [RavenRoute("admin/fs/{*id}")]
        public async Task<HttpResponseMessage> Put(string id, bool update = false)
        {
            MessageWithStatusCode nameFormatErrorMessage;
            if (IsValidName(id, SystemConfiguration.FileSystem.DataDirectory, out nameFormatErrorMessage) == false)
            {
                return GetMessageWithObject(new
                {
                    Error = nameFormatErrorMessage.Message
                }, nameFormatErrorMessage.ErrorCode);
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
                var existingFilesystem = SystemDatabase.Documents.Get(docKey, null);
                if (existingFilesystem != null)                   
                    return GetEmptyMessage(HttpStatusCode.Conflict);
            }

            var fsDoc = await ReadJsonObjectAsync<FileSystemDocument>().ConfigureAwait(false);
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

            SystemDatabase.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

        private static void EnsureFileSystemHasRequiredSettings(string id, FileSystemDocument fsDoc)
        {
            if (!fsDoc.Settings.ContainsKey(Constants.FileSystem.DataDirectory))
                fsDoc.Settings[Constants.FileSystem.DataDirectory] = "~/FileSystems/" + id;
        }

        [HttpDelete]
        [RavenRoute("admin/fs/{*id}")]
        public HttpResponseMessage Delete(string id)
        {
            if (id.StartsWith("batch-delete"))
            {
                return BatchDelete();
            }

            var isHardDeleteNeeded = ParseBoolQueryString("hard-delete");
            var message = DeleteFileSystem(id, isHardDeleteNeeded);
            if (message.ErrorCode != HttpStatusCode.OK)
            {
                return GetMessageWithString(message.Message, message.ErrorCode);
            }

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpDelete]
        [RavenRoute("admin/fs-batch-delete")]
        public HttpResponseMessage BatchDelete()
        {
            string[] fileSystemsToDelete = GetQueryStringValues("ids");
            if (fileSystemsToDelete == null)
            {
                return GetMessageWithString("No file systems to delete!", HttpStatusCode.BadRequest);
            }

            var isHardDeleteNeeded = ParseBoolQueryString("hard-delete");
            var successfullyDeletedDatabase = new List<string>();

            fileSystemsToDelete.ForEach(id =>
            {
                var message = DeleteFileSystem(id, isHardDeleteNeeded);
                if (message.ErrorCode == HttpStatusCode.OK)
                {
                    successfullyDeletedDatabase.Add(id);
                }
            });

            return GetMessageWithObject(successfullyDeletedDatabase.ToArray());
        }

        private const string ToggleDisablePrefix = "toggle-disable";

        [HttpPost]
        [RavenRoute("admin/fs/{*id}")]
        public async Task<HttpResponseMessage> OldToggleDisable(string id)
        {
            if (id.StartsWith(ToggleDisablePrefix))
            {
                string fsId = id.Substring(ToggleDisablePrefix.Length + 1);
                var isSettingToggleDisableStr = GetQueryStringValue("isSettingDisabled");
                bool isSettingToggleDisabled;
                if (!string.IsNullOrEmpty(isSettingToggleDisableStr) && bool.TryParse(isSettingToggleDisableStr, out isSettingToggleDisabled))
                {
                    return ToggleDisable(fsId, isSettingToggleDisabled);
                }
                return GetMessageWithString(string.Format("Failed to route call {0}", Request.RequestUri.OriginalString), HttpStatusCode.BadRequest);
            }

            if (id == "compact")
            {
                return Compact();
            }

            if (id == "restore")
            {
                return await Restore().ConfigureAwait(false);
            }

            var isSettingDisabledStr = GetQueryStringValue("isSettingDisabled");
            bool isSettingDisabled;
            if (!string.IsNullOrEmpty(isSettingDisabledStr) && bool.TryParse(isSettingDisabledStr, out isSettingDisabled))
            {
                return ToggleDisable(id, isSettingDisabled);
            }
            return GetMessageWithString(string.Format("Failed to route call {0}", Request.RequestUri.OriginalString), HttpStatusCode.BadRequest);
        }

        [HttpPost]
        [RavenRoute("admin/fs-toggle-disable")]
        public HttpResponseMessage ToggleDisable(string id, bool isSettingDisabled)
        {
            var message = ToggleFileSystemDisabled(id, isSettingDisabled);
            if (message.ErrorCode != HttpStatusCode.OK)
            {
                return GetMessageWithString(message.Message, message.ErrorCode);
            }

            return GetEmptyMessage();
        }


        [HttpPost]
        [RavenRoute("admin/fs-batch-toggle-disable")]
        public HttpResponseMessage BatchToggleDisable(bool isSettingDisabled)
        {
            string[] fileSystemsToToggle = GetQueryStringValues("ids");
            if (fileSystemsToToggle == null)
            {
                return GetMessageWithString("No file systems to toggle!", HttpStatusCode.BadRequest);
            }

            var successfullyToggledFileSystems = new List<string>();

            fileSystemsToToggle.ForEach(id =>
            {
                var message = ToggleFileSystemDisabled(id, isSettingDisabled);
                if (message.ErrorCode == HttpStatusCode.OK)
                {
                    successfullyToggledFileSystems.Add(id);
                }
            });

            return GetMessageWithObject(successfullyToggledFileSystems.ToArray());
        }

        private MessageWithStatusCode DeleteFileSystem(string id, bool isHardDeleteNeeded)
        {
            //get configuration even if the file system is disabled
            var configuration = FileSystemsLandlord.CreateTenantConfiguration(id, true);

            if (configuration == null)
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "File system wasn't found" };

            var docKey = Constants.FileSystem.Prefix + id;
            SystemDatabase.Documents.Delete(docKey, null, null);

            if (isHardDeleteNeeded && configuration.RunInMemory == false)
            {
                IOExtensions.DeleteDirectory(configuration.FileSystem.DataDirectory);

                if (configuration.FileSystem.IndexStoragePath != null)
                    IOExtensions.DeleteDirectory(configuration.FileSystem.IndexStoragePath);
            }

            return new MessageWithStatusCode();
        }

        private MessageWithStatusCode ToggleFileSystemDisabled(string id, bool isSettingDisabled)
        {
            var docKey = Constants.FileSystem.Prefix + id;
            var document = SystemDatabase.Documents.Get(docKey, null);
            if (document == null)
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "File system " + id + " wasn't found" };

            var doc = document.DataAsJson.JsonDeserialization<FileSystemDocument>();
            if (doc.Disabled == isSettingDisabled)
            {
                var state = isSettingDisabled ? "disabled" : "enabled";
                return new MessageWithStatusCode { ErrorCode = HttpStatusCode.BadRequest, Message = "File system " + id + " is already " + state };
            }

            doc.Disabled = !doc.Disabled;
            var json = RavenJObject.FromObject(doc);
            json.Remove("Id");
            SystemDatabase.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);

            return new MessageWithStatusCode();
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/admin/reset-index")]
        public HttpResponseMessage ResetIndex ()
        {
            FileSystem.Search.ForceIndexReset();
            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/admin/optimize-index")]
        public HttpResponseMessage Optimize()
        {
            FileSystem.Search.OptimizeIndex();

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("fs/admin/backup")]
        [RavenRoute("fs/{fileSystemName}/admin/backup")]
        public async Task<HttpResponseMessage> Backup()
        {
            var backupRequest = await ReadJsonObjectAsync<FilesystemBackupRequest>().ConfigureAwait(false);
            var incrementalString = InnerRequest.RequestUri.ParseQueryString()["incremental"];
            bool incrementalBackup;
            if (bool.TryParse(incrementalString, out incrementalBackup) == false)
                incrementalBackup = false;


            if (backupRequest.FileSystemDocument == null && FileSystem.Name != null)
            {
                var jsonDocument = DatabasesLandlord.SystemDatabase.Documents.Get(Constants.FileSystem.Prefix + FileSystem.Name, null);
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
                (bool.TryParse(FileSystem.Configuration.Settings["Raven/Esent/CircularLog"], out enableIncrementalBackup) == false || enableIncrementalBackup))
            {
                throw new InvalidOperationException("In order to run incremental backups using Esent you must have circular logging disabled");
            }

            if (incrementalBackup &&
                transactionalStorage is Storage.Voron.TransactionalStorage &&
                FileSystem.Configuration.Storage.Voron.AllowIncrementalBackups == false)
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

            var cts = new CancellationTokenSource();
            var state = new ResourceBackupState();

            var task = transactionalStorage.StartBackupOperation(DatabasesLandlord.SystemDatabase, FileSystem, backupDestinationDirectory, incrementalBackup, 
                filesystemDocument, state, cts.Token);

            task.ContinueWith(_ => cts.Dispose());

            long id;
            SystemDatabase.Tasks.AddTask(task, state, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.BackupFilesystem,
                Description = "Backup to: " + backupRequest.BackupLocation
            }, out id, cts);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
        }

        [HttpPost]
        [RavenRoute("admin/fs-compact")]
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
                    var targetFs = AsyncHelpers.RunSync(() => FileSystemsLandlord.GetResourceInternal(fs));
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
            SystemDatabase.Tasks.AddTask(task, new TaskBasedOperationState(task), new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.CompactFilesystem,
                Description = "Compact filesystem " + fs,
            }, out id);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
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
            if (ExistingDriveLetters.Contains(fullPath[0]))
                return true;

            message = GetMessageWithObject(new
            {
                Message = string.Format("Non-existing path : {0}", path)
            }, HttpStatusCode.BadRequest);
            return false;
        }

        [HttpPost]
        [RavenRoute("admin/fs-restore")]
        [RavenRoute("fs/{fileSystemName}/admin-restore")]
        public async Task<HttpResponseMessage> Restore()
        {
            var restoreStatus = new RestoreStatus { State = RestoreStatusState.Running, Messages = new List<string>() };

            var restoreRequest = await ReadJsonObjectAsync<FilesystemRestoreRequest>().ConfigureAwait(false);

            HttpResponseMessage message;
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
                SystemDatabase.Documents.Put(RestoreStatus.RavenFilesystemRestoreStatusDocumentKey(filesystemName), null, RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);

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
                                await Task.Delay(500, token).ConfigureAwait(false);
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
            SystemDatabase.Documents.Put(RestoreInProgress.RavenRestoreInProgressDocumentKey, null, RavenJObject.FromObject(new RestoreInProgress
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
                    filesystemDocument.Settings.Remove(Constants.RavenIndexPath);
                    filesystemDocument.Settings.Remove(Constants.RavenEsentLogsPath);
                    filesystemDocument.Settings.Remove(Constants.RavenTxJournalPath);

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
                    SystemDatabase.Documents.Delete(RestoreInProgress.RavenRestoreInProgressDocumentKey, null, null);
                }
            }, TaskCreationOptions.LongRunning);

            long id;
            SystemDatabase.Tasks.AddTask(task, new TaskBasedOperationState(task), new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.RestoreFilesystem,
                Description = "Restoring filesystem " + filesystemName + " from " + restoreRequest.BackupLocation
            }, out id);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
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

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/admin/synchronization/topology/view")]
        public Task<HttpResponseMessage> SynchronizationTopology()
        {
            var synchronizationTopologyDiscoverer = new SynchronizationTopologyDiscoverer(FileSystem, new RavenJArray(), 10, Log);
            var node = synchronizationTopologyDiscoverer.Discover();
            var topology = node.Flatten();

            return GetMessageWithObjectAsTask(topology);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/admin/changedbid")]
        public HttpResponseMessage ChangeDbId()
        {
            Guid old = FileSystem.Storage.Id;
            var newId = FileSystem.Storage.ChangeId();

            return GetMessageWithObject(new
            {
                OldId = old,
                NewId = newId
            });
        }
    }
}

// -----------------------------------------------------------------------
//  <copyright file="SynchronizationActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Database.FileSystem.Actions
{
    public class SynchronizationActions : ActionsBase
    {
        private readonly ConcurrentDictionary<Guid, ReaderWriterLockSlim> synchronizationFinishLocks = new ConcurrentDictionary<Guid, ReaderWriterLockSlim>();

        public SynchronizationActions(RavenFileSystem fileSystem, ILog log)
            : base(fileSystem, log)
        {
        }

        public void AssertFileIsNotBeingSynced(string fileName)
        {
            Storage.Batch(accessor =>
            {
                if (FileLockManager.TimeoutExceeded(fileName, accessor))
                {
                    FileLockManager.UnlockByDeletingSyncConfiguration(fileName, accessor);
                }
                else
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug("Cannot execute operation because file '{0}' is being synced", fileName);

                    throw new SynchronizationException(string.Format("File {0} is being synced", fileName));
                }
            });
        }

        public void FinishSynchronization(string fileName, SynchronizationReport report, FileSystemInfo sourceFileSystem, Etag sourceFileETag)
        {
            try
            {
                // we want to execute those operation in a single batch but we also have to ensure that
                // Raven/Synchronization/Sources/sourceServerId config is modified only by one finishing synchronization at the same time
                synchronizationFinishLocks.GetOrAdd(sourceFileSystem.Id, new ReaderWriterLockSlim()).EnterWriteLock();

                Storage.Batch(accessor =>
                {
                    SaveSynchronizationReport(fileName, accessor, report);
                    FileLockManager.UnlockByDeletingSyncConfiguration(fileName, accessor);

                    if (report.Exception == null)
                        SaveSynchronizationSourceInformation(sourceFileSystem, sourceFileETag);
                });
            }
            catch (Exception ex)
            {
                Log.ErrorException(string.Format("Failed to finish synchronization of a file '{0}' from {1}", fileName, sourceFileSystem), ex);
            }
            finally
            {
                synchronizationFinishLocks.GetOrAdd(sourceFileSystem.Id, new ReaderWriterLockSlim()).ExitWriteLock();
            }
        }

        private void SaveSynchronizationSourceInformation(FileSystemInfo sourceFileSystem, Etag lastSourceEtag, bool force = false)
        {
            var lastSynchronizationInformation = GetLastSynchronization(sourceFileSystem.Id);
            if (EtagUtil.IsGreaterThan(lastSynchronizationInformation.LastSourceFileEtag, lastSourceEtag) && force == false)
            {
                return;
            }

            var synchronizationSourceInfo = new SourceSynchronizationInformation
            {
                LastSourceFileEtag = lastSourceEtag,
                SourceServerUrl = sourceFileSystem.Url,
                DestinationServerId = Storage.Id
            };

            var key = SynchronizationConstants.RavenSynchronizationSourcesBasePath + "/" + sourceFileSystem.Id;

            Storage.Batch(accessor => accessor.SetConfig(key, JsonExtensions.ToJObject(synchronizationSourceInfo)));

            if (Log.IsDebugEnabled)
                Log.Debug("Saved last synchronized file ETag {0} from {1} ({2})", lastSourceEtag, sourceFileSystem.Url, sourceFileSystem.Id);
        }

        private void SaveSynchronizationReport(string fileName, IStorageActionsAccessor accessor, SynchronizationReport report)
        {
            var name = RavenFileNameHelper.SyncResultNameForFile(fileName);
            accessor.SetConfig(name, JsonExtensions.ToJObject(report));
        }

        public SourceSynchronizationInformation GetLastSynchronization(Guid from)
        {
            SourceSynchronizationInformation info = null;
            try
            {
                Storage.Batch(accessor =>
                {
                    info = accessor.GetConfig(SynchronizationConstants.RavenSynchronizationSourcesBasePath + "/" + from)
                               .JsonDeserialization<SourceSynchronizationInformation>();
                });
            }
            catch (FileNotFoundException)
            {
                info = new SourceSynchronizationInformation
                {
                    LastSourceFileEtag = Etag.Empty,
                    DestinationServerId = Storage.Id
                };
            }

            return info;
        }

        public void IncrementLastEtag(Guid sourceServerId, string sourceFileSystemUrl, string sourceFileETag, bool force)
        {
            try
            {
                // we want to execute those operation in a single batch but we also have to ensure that
                // Raven/Synchronization/Sources/sourceServerId config is modified only by one finishing synchronization at the same time
                synchronizationFinishLocks.GetOrAdd(sourceServerId, new ReaderWriterLockSlim()).EnterWriteLock();

                SaveSynchronizationSourceInformation(new FileSystemInfo
                {
                    Id = sourceServerId,
                    Url = sourceFileSystemUrl
                }, sourceFileETag, force);
            }
            catch (Exception ex)
            {
                Log.ErrorException(
                    string.Format("Failed to update last seen ETag from {0}", sourceServerId), ex);
            }
            finally
            {
                synchronizationFinishLocks.GetOrAdd(sourceServerId, new ReaderWriterLockSlim()).ExitWriteLock();
            }
        }

        public void DeleteSynchronizationReport(string fileName, IStorageActionsAccessor accessor)
        {
            var name = RavenFileNameHelper.SyncResultNameForFile(fileName);
            accessor.DeleteConfig(name);
            Search.Delete(name);
        }

        public SynchronizationReport GetSynchronizationReport(string fileName)
        {
            SynchronizationReport preResult = null;

            Storage.Batch(
                accessor =>
                {
                    try
                    {
                        var name = RavenFileNameHelper.SyncResultNameForFile(fileName);
                        preResult = accessor.GetConfig(name).JsonDeserialization<SynchronizationReport>();
                    }
                    catch (FileNotFoundException)
                    {
                        // just ignore
                    }
                });

            return preResult;
        }

        public RavenJObject GetLocalMetadata(string fileName)
        {
            RavenJObject result = null;
            try
            {
                Storage.Batch(accessor => { result = accessor.GetFile(fileName, 0, 0).Metadata; });
            }
            catch (FileNotFoundException)
            {
                return null;
            }

            if (result.ContainsKey(SynchronizationConstants.RavenDeleteMarker))
                return null;

            return result;
        }
    }
}

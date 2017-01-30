using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Json.Linq;

using Sparrow.Collections;

namespace Raven.Client.FileSystem.Changes
{

    public class FilesChangesClient : RemoteChangesClientBase<IFilesChanges, FilesConnectionState, FilesConvention>, IFilesChanges
    {
        private readonly static ILog Logger = LogManager.GetLogger(typeof(FilesChangesClient));

        private readonly ConcurrentSet<string> watchedFolders = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private bool watchAllConfigurations;
        private bool watchAllConflicts;
        private bool watchAllSynchronizations;

        private readonly Func<string, FileHeader, string, Action, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync;

        public FilesChangesClient(string url, string apiKey,
                                       ICredentials credentials,
                                       HttpJsonRequestFactory jsonRequestFactory, FilesConvention conventions,
                                       Func<string, FileHeader, string, Action, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync,
                                       Action onDispose)
            : base(url, apiKey, credentials, conventions, onDispose)
        {
            this.tryResolveConflictByUsingRegisteredConflictListenersAsync = tryResolveConflictByUsingRegisteredConflictListenersAsync;
        }

        public IObservableWithTask<ConfigurationChange> ForConfiguration()
        {
            var counter = GetOrAddConnectionState("all-fs-config", "watch-config", "unwatch-config",
                () => watchAllConfigurations = true,
                () => watchAllConfigurations = false,
                null);

            var taskedObservable = new TaskedObservable<ConfigurationChange, FilesConnectionState>(
                counter,
                notification => true);

            counter.OnConfigurationChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<ConflictChange> ForConflicts()
        {
            var counter = GetOrAddConnectionState("all-fs-conflicts", "watch-conflicts", "unwatch-conflicts",
                () => watchAllConflicts = true,
                () => watchAllConflicts = false,
                null);

            var taskedObservable = new TaskedObservable<ConflictChange, FilesConnectionState>(
                counter,
                notification => true);

            counter.OnConflictsNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<FileChange> ForFolder(string folder)
        {
            if (string.IsNullOrWhiteSpace(folder))
                throw new ArgumentException("folder cannot be empty");

            if (!folder.StartsWith("/"))
                throw new ArgumentException("folder must start with /");

            var canonicalisedFolder = folder.TrimStart('/');
            var key = "fs-folder/" + canonicalisedFolder;
            var counter = GetOrAddConnectionState(key, "watch-folder", "unwatch-folder",
                () => watchedFolders.TryAdd(folder),
                () => watchedFolders.TryRemove(folder),
                folder);

            var taskedObservable = new TaskedObservable<FileChange, FilesConnectionState>(
                counter,
                notification => notification.File.StartsWith(folder, StringComparison.OrdinalIgnoreCase));

            counter.OnFileChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<SynchronizationUpdateChange> ForSynchronization()
        {
            var counter = GetOrAddConnectionState("all-fs-sync", "watch-sync", "unwatch-sync",
                () => watchAllSynchronizations = true,
                () => watchAllSynchronizations = false,
                null);

            var taskedObservable = new TaskedObservable<SynchronizationUpdateChange, FilesConnectionState>(
                counter,
                notification => true);

            counter.OnSynchronizationNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        protected override async Task SubscribeOnServer()
        {
            if (watchAllConfigurations)
                await Send("watch-config", null).ConfigureAwait(false);

            if (watchAllConflicts)
                await Send("watch-conflicts", null).ConfigureAwait(false);

            if (watchAllSynchronizations)
                await Send("watch-sync", null).ConfigureAwait(false);

            foreach (var watchedFolder in watchedFolders)
            {
                await Send("watch-folder", watchedFolder).ConfigureAwait(false);
            }
        }

        private ConcurrentDictionary<string, ConflictChange> delayedConflictNotifications = new ConcurrentDictionary<string, ConflictChange>();

        protected override void NotifySubscribers(string type, RavenJObject value, List<FilesConnectionState> connections)
        {
            switch (type)
            {
                case "ConfigurationChange":
                    var configChangeNotification = value.JsonDeserialization<ConfigurationChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(configChangeNotification);
                    }
                    break;
                case "FileChange":
                    var fileChangeNotification = value.JsonDeserialization<FileChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(fileChangeNotification);
                    }
                    break;
                case "SynchronizationUpdateChange":
                    var synchronizationUpdateNotification = value.JsonDeserialization<SynchronizationUpdateChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(synchronizationUpdateNotification);
                    }
                    break;

                case "ConflictChange":
                    var conflictNotification = value.JsonDeserialization<ConflictChange>();
                    if (conflictNotification.Status == ConflictStatus.Detected)
                    {
                        // We don't care about this one (this can happen concurrently). 
                        delayedConflictNotifications.AddOrUpdate(conflictNotification.FileName, conflictNotification, (x, y) => conflictNotification);

                        tryResolveConflictByUsingRegisteredConflictListenersAsync(conflictNotification.FileName, 
                                                                                  conflictNotification.RemoteFileHeader, 
                                                                                  conflictNotification.SourceServerUrl,
                                                                                  () => NotifyConflictSubscribers(connections, conflictNotification))                             
                            .ContinueWith(t =>
                            {
                                t.AssertNotFailed();

                                // We need the lock to avoid a race conditions where a Detected happens and also a Resolved happen before the continuation can take control.. 
                                lock ( delayedConflictNotifications )
                                {
                                    ConflictChange change;
                                    if (delayedConflictNotifications.TryRemove(conflictNotification.FileName, out change))
                                    {
                                        if (change.Status == ConflictStatus.Resolved)
                                            NotifyConflictSubscribers(connections, change);
                                    }
                                }

                                if (t.Result)
                                {
                                    if (Logger.IsDebugEnabled)
                                        Logger.Debug("Document replication conflict for {0} was resolved by one of the registered conflict listeners", conflictNotification.FileName);
                                }
                            }).ConfigureAwait(false);
                    }
                    else if (conflictNotification.Status == ConflictStatus.Resolved )
                    {
                        // We need the lock to avoid race conditions. 
                        lock ( delayedConflictNotifications )
                        {
                            if (delayedConflictNotifications.ContainsKey(conflictNotification.FileName))
                            {
                                delayedConflictNotifications.AddOrUpdate(conflictNotification.FileName, conflictNotification, (x, y) => conflictNotification);

                                // We are delaying broadcasting.
                                conflictNotification = null;
                            }
                            else NotifyConflictSubscribers(connections, conflictNotification);
                        }
                    }

                    break;
                default:
                    break;
            }
        }

        private static void NotifyConflictSubscribers(List<FilesConnectionState> connections, ConflictChange conflictChange)
        {
            // Check if we are delaying the broadcast.
            if (conflictChange != null)
            {
                foreach (var counter in connections)
                    counter.Send(conflictChange);
            }
        }
        }
    }

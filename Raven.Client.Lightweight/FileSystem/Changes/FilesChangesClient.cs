using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Database.Util;
using Raven.Json.Linq;
using Sparrow.Collections;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Changes
{

    public class FilesChangesClient : RemoteChangesClientBase<IFilesChanges, FilesConnectionState>,
                                        IFilesChanges,
                                        IHoldProfilingInformation
    {
#if !DNXCORE50
        private readonly static ILog logger = LogManager.GetCurrentClassLogger();
#else
        private readonly static ILog logger = LogManager.GetLogger(typeof(FilesChangesClient));
#endif

        private readonly ConcurrentSet<string> watchedFolders = new ConcurrentSet<string>();

        private bool watchAllConfigurations;
        private bool watchAllConflicts;
        private bool watchAllSynchronizations;

        private readonly Func<string, FileHeader, string, Action, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync;

        public ProfilingInformation ProfilingInformation { get; private set; }

        public FilesChangesClient(string url, string apiKey,
                                       ICredentials credentials,
                                       HttpJsonRequestFactory jsonRequestFactory, FilesConvention conventions,
                                       IReplicationInformerBase replicationInformer,
                                       Func<string, FileHeader, string, Action, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync,
                                       Action onDispose)
            : base(url, apiKey, credentials, jsonRequestFactory, conventions, replicationInformer, onDispose)
        {
            this.tryResolveConflictByUsingRegisteredConflictListenersAsync = tryResolveConflictByUsingRegisteredConflictListenersAsync;
        }

        public IObservableWithTask<ConfigurationChangeNotification> ForConfiguration()
        {
            var counter = Counters.GetOrAdd("all-fs-config", s =>
            {
                var configurationSubscriptionTask = AfterConnection(() =>
                {
                    watchAllConfigurations = true;
                    return Send("watch-config", null);
                });
                return new FilesConnectionState(
                    () =>
                    {
                        watchAllConfigurations = false;
                        Send("unwatch-config", null);
                        Counters.Remove("all-fs-config");
                    },
                    configurationSubscriptionTask);
            });

            var taskedObservable = new TaskedObservable<ConfigurationChangeNotification, FilesConnectionState>(
                counter,
                notification => true);

            counter.OnConfigurationChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<ConflictNotification> ForConflicts()
        {
            var counter = Counters.GetOrAdd("all-fs-conflicts", s =>
            {
                var conflictsSubscriptionTask = AfterConnection(() =>
                {
                    watchAllConflicts = true;
                    return Send("watch-conflicts", null);
                });
                return new FilesConnectionState(
                    () =>
                    {
                        watchAllConflicts = false;
                        Send("unwatch-conflicts", null);
                        Counters.Remove("all-fs-conflicts");
                    },
                    conflictsSubscriptionTask);
            });
            var taskedObservable = new TaskedObservable<ConflictNotification, FilesConnectionState>(
                counter,
                notification => true);

            counter.OnConflictsNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<FileChangeNotification> ForFolder(string folder)
        {
            if (string.IsNullOrWhiteSpace(folder))
                throw new ArgumentException("folder cannot be empty");

            if (!folder.StartsWith("/"))
                throw new ArgumentException("folder must start with /");

            var canonicalisedFolder = folder.TrimStart('/');

            // watch-folder, unwatch-folder

            var counter = Counters.GetOrAdd("fs-folder/" + canonicalisedFolder, s =>
            {
                var fileChangeSubscriptionTask = AfterConnection(() =>
                {
                    watchedFolders.TryAdd(folder);
                    return Send("watch-folder", folder);
                });

                return new FilesConnectionState(
                    () =>
                    {
                        watchedFolders.TryRemove(folder);
                        Send("unwatch-folder", folder);
                        Counters.Remove("fs-folder/" + canonicalisedFolder);
                    },
                    fileChangeSubscriptionTask);
            });

            var taskedObservable = new TaskedObservable<FileChangeNotification, FilesConnectionState>(
                counter,
                notification => notification.File.StartsWith(folder, StringComparison.OrdinalIgnoreCase));

            counter.OnFileChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<SynchronizationUpdateNotification> ForSynchronization()
        {
            var counter = Counters.GetOrAdd("all-fs-sync", s =>
            {
                var conflictsSubscriptionTask = AfterConnection(() =>
                {
                    watchAllSynchronizations = true;
                    return Send("watch-sync", null);
                });
                return new FilesConnectionState(
                    () =>
                    {
                        watchAllSynchronizations = false;
                        Send("unwatch-sync", null);
                        Counters.Remove("all-fs-sync");
                    },
                    conflictsSubscriptionTask);
            });
            var taskedObservable = new TaskedObservable<SynchronizationUpdateNotification, FilesConnectionState>(
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

        private ConcurrentDictionary<string, ConflictNotification> delayedConflictNotifications = new ConcurrentDictionary<string, ConflictNotification>();

        protected override void NotifySubscribers(string type, RavenJObject value, List<FilesConnectionState> connections)
        {
            switch (type)
            {
                case "ConfigurationChangeNotification":
                    var configChangeNotification = value.JsonDeserialization<ConfigurationChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(configChangeNotification);
                    }
                    break;
                case "FileChangeNotification":
                    var fileChangeNotification = value.JsonDeserialization<FileChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(fileChangeNotification);
                    }
                    break;
                case "SynchronizationUpdateNotification":
                    var synchronizationUpdateNotification = value.JsonDeserialization<SynchronizationUpdateNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(synchronizationUpdateNotification);
                    }
                    break;

                case "ConflictNotification":
                    var conflictNotification = value.JsonDeserialization<ConflictNotification>();
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
                                    ConflictNotification notification;
                                    if (delayedConflictNotifications.TryRemove(conflictNotification.FileName, out notification))
                                    {
                                        if (notification.Status == ConflictStatus.Resolved)
                                            NotifyConflictSubscribers(connections, notification);
                                    }
                                }

                                if (t.Result)
                                {
                                    logger.Debug("Document replication conflict for {0} was resolved by one of the registered conflict listeners", conflictNotification.FileName);
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

        private static void NotifyConflictSubscribers(List<FilesConnectionState> connections, ConflictNotification conflictNotification)
        {
            // Check if we are delaying the broadcast.
            if (conflictNotification != null)
            {
                foreach (var counter in connections)
                    counter.Send(conflictNotification);
            }
        }


        private Task AfterConnection(Func<Task> action)
        {
            return Task.ContinueWith(task =>
            {
                task.AssertNotFailed();
                return action();
            })
            .Unwrap();
        }
    }
}

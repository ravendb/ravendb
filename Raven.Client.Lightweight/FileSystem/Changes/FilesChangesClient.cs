using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Changes;
using Raven.Client.FileSystem.Connection;
using Raven.Database.Util;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Changes
{

    public class FilesChangesClient : RemoteChangesClientBase<IFilesChanges, FilesConnectionState>,
                                        IFilesChanges,
                                        IHoldProfilingInformation
    {
        private readonly ConcurrentSet<string> watchedFolders = new ConcurrentSet<string>();

        private bool watchAllConfigurations;
        private bool watchAllConflicts;
        private bool watchAllSynchronizations;
        private bool watchAllCancellations;

        public ProfilingInformation ProfilingInformation { get; private set; }

        public FilesChangesClient(string url, string apiKey,
                                       ICredentials credentials,
                                       HttpJsonRequestFactory jsonRequestFactory, FilesConvention conventions,
                                       IReplicationInformerBase replicationInformer,
                                       Action onDispose)
            : base(url, apiKey, credentials, jsonRequestFactory, conventions, replicationInformer, onDispose)
        {
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
                notification => notification.File.StartsWith(folder, StringComparison.InvariantCultureIgnoreCase));

            counter.OnFileChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<CancellationNotification> ForCancellations()
        {
            // watch-cancellations, unwatch-cancellations
            var counter = Counters.GetOrAdd("all-fs-cancellations", s =>
            {
                var cancellationsSubscriptionTask = AfterConnection(() =>
                {
                    watchAllCancellations = true;
                    return Send("watch-cancellations", null);
                });
                return new FilesConnectionState(
                    () =>
                    {
                        watchAllCancellations = false;
                        Send("unwatch-cancellations", null);
                        Counters.Remove("all-fs-cancellations");
                    },
                    cancellationsSubscriptionTask);
            });
            var taskedObservable = new TaskedObservable<CancellationNotification, FilesConnectionState>(
                counter,
                notification => true);

            counter.OnCancellationNotification += taskedObservable.Send;
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

            if (watchAllCancellations)
                await Send("watch-cancellations", null).ConfigureAwait(false);

            foreach (var watchedFolder in watchedFolders)
            {
                await Send("watch-folder", watchedFolder).ConfigureAwait(false);
            }
        }

        protected override void NotifySubscribers(string type, RavenJObject value, IEnumerable<KeyValuePair<string, FilesConnectionState>> connections)
        {
            switch (type)
            {
                case "ConfigurationChangeNotification":
                    var configChangeNotification = value.JsonDeserialization<ConfigurationChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(configChangeNotification);
                    }
                    break;
                case "FileChangeNotification":
                    var fileChangeNotification = value.JsonDeserialization<FileChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(fileChangeNotification);
                    }
                    break;
                case "SynchronizationUpdateNotification":
                    var synchronizationUpdateNotification = value.JsonDeserialization<SynchronizationUpdateNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(synchronizationUpdateNotification);
                    }
                    break;
                case "CancellationNotification":
                    var uploadFailedNotification = value.JsonDeserialization<CancellationNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(uploadFailedNotification);
                    }
                    break;

                case "ConflictNotification":
                    var conflictNotification = value.JsonDeserialization<ConflictNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(conflictNotification);
                    }
                    break;
                case "ConflictDetectedNotification":
                    var conflictDetectedNotification = value.JsonDeserialization<ConflictNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(conflictDetectedNotification);
                    }
                    break;
                case "ConflictResolvedNotification":
                     var conflictResolvedNotification = value.JsonDeserialization<ConflictNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(conflictResolvedNotification);
                    }
                    break;
                default:
                    break;
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

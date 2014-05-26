using System;
using System.Linq;
using System.Net;
using System.Security.Policy;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
#if NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Client.RavenFS.Changes
{
    using Raven.Abstractions.Connection;
    using Raven.Client.Changes;
    using Raven.Client.RavenFS.Connections;
    using System.Collections.Generic;
    using Raven.Client.Connection.Profiling;
    using System.Diagnostics;

    public class RemoteFileSystemChanges : RemoteChangesClientBase<IFileSystemChanges, FileSystemConnectionState>, 
                                           IFileSystemChanges,
                                           IHoldProfilingInformation
    {
        private readonly ConcurrentSet<string> watchedFolders = new ConcurrentSet<string>();

        private bool watchAllConfigurations;
        private bool watchAllConflicts;
        private bool watchAllSynchronizations;
        private bool watchAllCancellations;

        public ProfilingInformation ProfilingInformation { get; private set; }

        public RemoteFileSystemChanges(string url, string apiKey,
                                       ICredentials credentials,
                                       HttpJsonRequestFactory jsonRequestFactory, FileConvention conventions,
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
                return new FileSystemConnectionState(
                    () =>
                    {
                        watchAllConfigurations = false;
                        Send("unwatch-config", null);
                        Counters.Remove("all-fs-config");
                    },
                    configurationSubscriptionTask);
            });
            var taskedObservable = new TaskedObservable<ConfigurationChangeNotification, FileSystemConnectionState>(
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
                return new FileSystemConnectionState(
                    () =>
                    {
                        watchAllConflicts = false;
                        Send("unwatch-conflicts", null);
                        Counters.Remove("all-fs-conflicts");
                    },
                    conflictsSubscriptionTask);
            });
            var taskedObservable = new TaskedObservable<ConflictNotification, FileSystemConnectionState>(
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

                return new FileSystemConnectionState(
                    () =>
                    {
                        watchedFolders.TryRemove(folder);
                        Send("unwatch-folder", folder);
                        Counters.Remove("fs-folder/" + canonicalisedFolder);
                    },
                    fileChangeSubscriptionTask);
            });

            var taskedObservable = new TaskedObservable<FileChangeNotification, FileSystemConnectionState>(
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
                return new FileSystemConnectionState(
                    () =>
                    {
                        watchAllCancellations = false;
                        Send("unwatch-cancellations", null);
                        Counters.Remove("all-fs-cancellations");
                    },
                    cancellationsSubscriptionTask);
            });
            var taskedObservable = new TaskedObservable<CancellationNotification, FileSystemConnectionState>(
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
                return new FileSystemConnectionState(
                    () =>
                    {
                        watchAllSynchronizations = false;
                        Send("unwatch-sync", null);
                        Counters.Remove("all-fs-sync");
                    },
                    conflictsSubscriptionTask);
            });
            var taskedObservable = new TaskedObservable<SynchronizationUpdateNotification, FileSystemConnectionState>(
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

        protected override void NotifySubscribers(string type, RavenJObject value, IEnumerable<KeyValuePair<string, FileSystemConnectionState>> connections)
        {
            switch( type )
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
                    var conflictDetectedNotification = value.JsonDeserialization<ConflictDetectedNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(conflictDetectedNotification);
                    }
                    break;
                case "ConflictResolvedNotification":
                     var conflictResolvedNotification = value.JsonDeserialization<ConflictResolvedNotification>();
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

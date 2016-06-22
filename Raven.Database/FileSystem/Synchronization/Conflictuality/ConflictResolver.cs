using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization.Conflictuality.Resolvers;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization.Conflictuality
{
    public class ConflictResolver
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly CompositionContainer container;
        private readonly ITransactionalStorage storage;

        public ConflictResolver(ITransactionalStorage storage, CompositionContainer container)
        {
            this.container = container;
            this.storage = storage;
        }

        public IEnumerable<AbstractFileSynchronizationConflictResolver> Resolvers
        {
            get
            {
                var exported = container.GetExportedValues<AbstractFileSynchronizationConflictResolver>();

                var config = SynchronizationConfigAccessor.GetOrDefault(storage);

                if (config == null || config.FileConflictResolution == StraightforwardConflictResolution.None)
                    return exported;

                var withConfiguredResolvers = exported.ToList();

                switch (config.FileConflictResolution)
                {
                    case StraightforwardConflictResolution.ResolveToLocal:
                        withConfiguredResolvers.Add(LocalFileSynchronizationConflictResolver.Instance);
                        break;
                    case StraightforwardConflictResolution.ResolveToRemote:
                        withConfiguredResolvers.Add(RemoveFileSynchronizationConflictResolver.Instance);
                        break;
                    case StraightforwardConflictResolution.ResolveToLatest:
                        withConfiguredResolvers.Add(LatestFileSynchronizationConflictResolver.Instance);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("config.FileConflictResolution");
                }

                return withConfiguredResolvers;
            }
        }


        public bool TryResolveConflict(string fileName, ConflictItem conflict, RavenJObject localMetadata, RavenJObject remoteMetadata, out ConflictResolutionStrategy strategy)
        {
            foreach (var resolver in Resolvers)
            {
                if (resolver.TryResolve(fileName, localMetadata, remoteMetadata, out strategy) == false)
                    continue;

                switch (strategy)
                {
                    case ConflictResolutionStrategy.CurrentVersion:
                        ApplyCurrentStrategy(fileName, conflict, localMetadata);
                        return true;
                    case ConflictResolutionStrategy.RemoteVersion:
                        ApplyRemoteStrategy(fileName, conflict, localMetadata);
                        return true;
                }
            }

            strategy = ConflictResolutionStrategy.NoResolution;
            return false;
        }

        public bool CheckIfResolvedByRemoteStrategy(RavenJObject destinationMetadata, ConflictItem conflict)
        {
            var conflictResolutionMetadata = destinationMetadata[SynchronizationConstants.RavenSynchronizationConflictResolution] as RavenJObject;
            if (conflictResolutionMetadata == null)
                return false;

            var conflictResolution = JsonExtensions.JsonDeserialization<ConflictResolution>(conflictResolutionMetadata);
            return conflictResolution.Strategy == ConflictResolutionStrategy.RemoteVersion && conflictResolution.RemoteServerId == conflict.RemoteHistory.Last().ServerId;
        }

        public void ApplyCurrentStrategy(string fileName, ConflictItem conflict, RavenJObject localMetadata)
        {
            var localHistory = Historian.DeserializeHistory(localMetadata);

            // incorporate remote version history into local
            foreach (var remoteHistoryItem in conflict.RemoteHistory.Where(remoteHistoryItem => !localHistory.Contains(remoteHistoryItem)))
            {
                localHistory.Add(remoteHistoryItem);
            }

            localMetadata[SynchronizationConstants.RavenSynchronizationHistory] = Historian.SerializeHistory(localHistory);
        }

        public void ApplyRemoteStrategy(string fileName, ConflictItem conflict, RavenJObject localMetadata)
        {
            var conflictResolution = new ConflictResolution
            {
                Strategy = ConflictResolutionStrategy.RemoteVersion,
                RemoteServerId = conflict.RemoteHistory.Last().ServerId,
                Version = conflict.RemoteHistory.Last().Version,
            };

            conflict.ResolveUsingRemote = true;

            localMetadata[SynchronizationConstants.RavenSynchronizationConflictResolution] = JsonExtensions.ToJObject(conflictResolution);
        }
    }
}

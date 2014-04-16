using System;
using System.Collections.Specialized;
using System.Linq;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Synchronization.Conflictuality
{
	public class ConflictResolver
	{
        public bool IsResolved(NameValueCollection destinationMetadata, ConflictItem conflict)
        {
            var conflictResolutionString = destinationMetadata[SynchronizationConstants.RavenSynchronizationConflictResolution];
            if (String.IsNullOrEmpty(conflictResolutionString))
                return false;

            var conflictResolution = new TypeHidingJsonSerializer().Parse<ConflictResolution>(conflictResolutionString);
            return conflictResolution.Strategy == ConflictResolutionStrategy.RemoteVersion && conflictResolution.RemoteServerId == conflict.RemoteHistory.Last().ServerId;
        }

        public bool IsResolved(RavenJObject destinationMetadata, ConflictItem conflict)
        {
            var conflictResolutionString = destinationMetadata.Value<string>(SynchronizationConstants.RavenSynchronizationConflictResolution);
            if (String.IsNullOrEmpty(conflictResolutionString))
                return false;

            var conflictResolution = new TypeHidingJsonSerializer().Parse<ConflictResolution>(conflictResolutionString);
            return conflictResolution.Strategy == ConflictResolutionStrategy.RemoteVersion && conflictResolution.RemoteServerId == conflict.RemoteHistory.Last().ServerId;
        }
	}
}

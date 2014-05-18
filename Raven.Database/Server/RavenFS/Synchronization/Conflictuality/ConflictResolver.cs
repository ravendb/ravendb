using System;
using System.Collections.Specialized;
using System.Linq;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.RavenFS.Synchronization.Conflictuality
{
	public class ConflictResolver
	{
        public bool IsResolved(RavenJObject destinationMetadata, ConflictItem conflict)
        {
            var conflictResolutionMetadata = destinationMetadata[SynchronizationConstants.RavenSynchronizationConflictResolution] as RavenJObject;
            if (conflictResolutionMetadata == null)
                return false;

            var conflictResolution = JsonExtensions.JsonDeserialization<ConflictResolution>(conflictResolutionMetadata);            
            return conflictResolution.Strategy == ConflictResolutionStrategy.RemoteVersion && conflictResolution.RemoteServerId == conflict.RemoteHistory.Last().ServerId;
        }
	}
}

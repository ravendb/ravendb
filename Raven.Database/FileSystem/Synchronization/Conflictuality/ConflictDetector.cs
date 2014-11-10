using System.Collections.Generic;
using System.Collections.Specialized;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Json.Linq;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization.Conflictuality
{
	public class ConflictDetector
	{
        public ConflictItem Check(string fileName, RavenJObject localMetadata, RavenJObject remoteMetadata, string remoteServerUrl)
		{
			if (Historian.IsDirectChildOfCurrent(localMetadata, remoteMetadata))
				return null;

			return
				new ConflictItem
				{
					CurrentHistory = TransformToFullConflictHistory(localMetadata),
					RemoteHistory = TransformToFullConflictHistory(remoteMetadata),
					FileName = fileName,
					RemoteServerUrl = remoteServerUrl
				};
		}

        public ConflictItem CheckOnSource(string fileName, RavenJObject localMetadata, RavenJObject remoteMetadata, string localServerUrl)
		{
			return Check(fileName, remoteMetadata, localMetadata, localServerUrl);
		}

		private static List<HistoryItem> TransformToFullConflictHistory(RavenJObject metadata)
		{
            var version = metadata.Value<long>(SynchronizationConstants.RavenSynchronizationVersion);
			var serverId = metadata.Value<string>(SynchronizationConstants.RavenSynchronizationSource);
			var fullHistory = Historian.DeserializeHistory(metadata);
			fullHistory.Add(new HistoryItem { ServerId = serverId, Version = version });

			return fullHistory;
		}
	}
}
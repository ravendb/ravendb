using System.Collections.Generic;
using System.Collections.Specialized;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Infrastructure;

namespace Raven.Database.Server.RavenFS.Synchronization.Conflictuality
{
	public class ConflictDetector
	{
		public ConflictItem Check(string fileName, NameValueCollection localMetadata, NameValueCollection remoteMetadata,
								  string remoteServerUrl)
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

		public ConflictItem CheckOnSource(string fileName, NameValueCollection localMetadata,
										  NameValueCollection remoteMetadata, string localServerUrl)
		{
			return Check(fileName, remoteMetadata, localMetadata, localServerUrl);
		}

		private static List<HistoryItem> TransformToFullConflictHistory(NameValueCollection metadata)
		{
			var version = long.Parse(metadata[SynchronizationConstants.RavenSynchronizationVersion]);
			var serverId = metadata[SynchronizationConstants.RavenSynchronizationSource];
			var fullHistory = Historian.DeserializeHistory(metadata);
			fullHistory.Add(new HistoryItem { ServerId = serverId, Version = version });

			return fullHistory;
		}
	}
}
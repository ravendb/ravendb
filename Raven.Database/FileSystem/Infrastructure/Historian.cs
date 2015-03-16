using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Data;

namespace Raven.Database.FileSystem.Infrastructure
{
	public class Historian
	{
		private readonly ITransactionalStorage storage;
		private readonly SynchronizationHiLo synchronizationHiLo;

		public Historian(ITransactionalStorage storage, SynchronizationHiLo synchronizationHiLo)
		{
			this.storage = storage;
			this.synchronizationHiLo = synchronizationHiLo;
		}

		public void Update(string fileName, RavenJObject sourceMetadata)
		{
			var fileMetadata = GetMetadata(fileName);
            var serverId = fileMetadata.Value<string>(SynchronizationConstants.RavenSynchronizationSource);
			var history = new List<HistoryItem>();
			// if there is RavenReplicationVersion metadata it means that file is not new and we have to add a new item to the history
			if (!String.IsNullOrEmpty(serverId))
			{
				var currentVersion = fileMetadata.Value<long>(SynchronizationConstants.RavenSynchronizationVersion);               
                history = DeserializeHistory(fileMetadata);
				history.Add(new HistoryItem { ServerId = serverId, Version = currentVersion });
			}

			if (history.Count > SynchronizationConstants.ChangeHistoryLength)
				history.RemoveAt(0);

            sourceMetadata[SynchronizationConstants.RavenSynchronizationHistory] = SerializeHistory(history);            
			sourceMetadata[SynchronizationConstants.RavenSynchronizationVersion] = synchronizationHiLo.NextId();
			sourceMetadata[SynchronizationConstants.RavenSynchronizationSource] = new RavenJValue(storage.Id);
		}

        public void UpdateLastModified(RavenJObject metadata)
        {
            UpdateLastModified(metadata, DateTimeOffset.UtcNow);
        }

        public void UpdateLastModified(RavenJObject metadata, DateTimeOffset when)
        {
            // internally keep last modified date with millisecond precision
            metadata[Constants.LastModified] = when;
            metadata[Constants.RavenLastModified] = when;
        }

        private RavenJObject GetMetadata(string fileName)
		{
            try
            {
                FileAndPagesInformation fileAndPages = null;
                storage.Batch(accessor => fileAndPages = accessor.GetFile(fileName, 0, 0));
                return fileAndPages.Metadata;
            }
            catch (FileNotFoundException)
            {
                return new RavenJObject();
            }
		}

        public static List<HistoryItem> DeserializeHistory(RavenJObject metadata)
        {
            var history = new List<HistoryItem>();
            if (metadata.ContainsKey(SynchronizationConstants.RavenSynchronizationHistory))
            {
                var array = (RavenJArray) metadata[SynchronizationConstants.RavenSynchronizationHistory];
                var items = array.Values<RavenJObject>().Select(x => x.JsonDeserialization<HistoryItem>());
                return new List<HistoryItem>(items);
            }

            return history;
        }

        public static RavenJArray SerializeHistory(List<HistoryItem> history)
        {
            return JsonExtensions.ToJArray(history);
        }

        public static bool IsDirectChildOfCurrent(RavenJObject destinationMetadata, RavenJObject sourceMetadata)
		{
            long destVersion = destinationMetadata.Value<long>(SynchronizationConstants.RavenSynchronizationVersion);
            var destServerId = destinationMetadata.Value<string>(SynchronizationConstants.RavenSynchronizationSource);

			var version = new HistoryItem { ServerId = destServerId, Version = destVersion };

			var history = DeserializeHistory(sourceMetadata);
            long sourceVersion = sourceMetadata.Value<long>(SynchronizationConstants.RavenSynchronizationVersion);
			var sourceServerId = sourceMetadata.Value<string>(SynchronizationConstants.RavenSynchronizationSource);

			history.Add(new HistoryItem { ServerId = sourceServerId, Version = sourceVersion });

			return history.Contains(version);
		}
	}
}
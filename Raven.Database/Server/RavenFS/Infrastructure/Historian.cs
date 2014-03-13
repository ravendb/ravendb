using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Synchronization;

namespace Raven.Database.Server.RavenFS.Infrastructure
{
	public class Historian
	{
		private readonly ITransactionalStorage storage;
		private readonly SynchronizationHiLo synchronizationHiLo;
		private readonly UuidGenerator uuidGenerator;

		public Historian(ITransactionalStorage storage, SynchronizationHiLo synchronizationHiLo, UuidGenerator uuidGenerator)
		{
			this.storage = storage;
			this.uuidGenerator = uuidGenerator;
			this.synchronizationHiLo = synchronizationHiLo;
		}

		public void Update(string fileName, NameValueCollection nameValueCollection)
		{
			var metadata = GetMetadata(fileName);
			var serverId = metadata[SynchronizationConstants.RavenSynchronizationSource];
			var history = new List<HistoryItem>();
			// if there is RavenReplicationVersion metadata it means that file is not new and we have to add a new item to the history
			if (!String.IsNullOrEmpty(serverId))
			{
				var currentVersion = long.Parse(metadata[SynchronizationConstants.RavenSynchronizationVersion]);
				history = DeserializeHistory(metadata);
				history.Add(new HistoryItem { ServerId = serverId, Version = currentVersion });
			}

			if (history.Count > SynchronizationConstants.ChangeHistoryLength)
				history.RemoveAt(0);

			nameValueCollection[SynchronizationConstants.RavenSynchronizationHistory] = SerializeHistory(history);
			nameValueCollection[SynchronizationConstants.RavenSynchronizationVersion] =
				synchronizationHiLo.NextId().ToString(CultureInfo.InvariantCulture);
			nameValueCollection[SynchronizationConstants.RavenSynchronizationSource] = storage.Id.ToString();
		}


		public void UpdateLastModified(NameValueCollection nameValueCollection)
		{
			// internally keep last modified date with millisecond precision
			nameValueCollection["Last-Modified"] = DateTime.UtcNow.ToString("d MMM yyyy H:m:s.fffff 'GMT'",
																			CultureInfo.InvariantCulture);
			nameValueCollection["ETag"] = "\"" + uuidGenerator.CreateSequentialUuid() + "\"";
		}

		private NameValueCollection GetMetadata(string fileName)
		{
			try
			{
				FileAndPages fileAndPages = null;
				storage.Batch(accessor => fileAndPages = accessor.GetFile(fileName, 0, 0));
				return fileAndPages.Metadata;
			}
			catch (FileNotFoundException)
			{
				return new NameValueCollection();
			}
		}

		public static List<HistoryItem> DeserializeHistory(NameValueCollection nameValueCollection)
		{
			var serializedHistory = nameValueCollection[SynchronizationConstants.RavenSynchronizationHistory];
			return serializedHistory == null ? null : new JsonSerializer().Deserialize<List<HistoryItem>>(new JsonTextReader(new StringReader(serializedHistory)));
		}

		public static string SerializeHistory(List<HistoryItem> history)
		{
			var sb = new StringBuilder();
			var jw = new JsonTextWriter(new StringWriter(sb));
			new JsonSerializer().Serialize(jw, history);
			return sb.ToString();
		}

		public static bool IsDirectChildOfCurrent(NameValueCollection destinationMetadata, NameValueCollection sourceMetadata)
		{
			var destVersion = long.Parse(destinationMetadata[SynchronizationConstants.RavenSynchronizationVersion]);
			var destServerId = destinationMetadata[SynchronizationConstants.RavenSynchronizationSource];

			var version = new HistoryItem { ServerId = destServerId, Version = destVersion };

			var history = DeserializeHistory(sourceMetadata) ?? new List<HistoryItem>();
			var sourceVersion = long.Parse(sourceMetadata[SynchronizationConstants.RavenSynchronizationVersion]);
			var sourceServerId = sourceMetadata[SynchronizationConstants.RavenSynchronizationSource];

			history.Add(new HistoryItem { ServerId = sourceServerId, Version = sourceVersion });

			return history.Contains(version);
		}
	}
}
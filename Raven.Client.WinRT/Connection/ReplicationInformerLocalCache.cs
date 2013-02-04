using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Windows.Storage;

namespace Raven.Client.Connection
{
	public static class ReplicationInformerLocalCache
	{
		private readonly static ILog log = LogManager.GetCurrentClassLogger();
		private static readonly StorageFolder folder = ApplicationData.Current.RoamingFolder;

		public static JsonDocument TryLoadReplicationInformationFromLocalCache(string serverHash)
		{
			try
			{
				return Load(serverHash).Result;
			}
			catch (Exception e)
			{
				log.ErrorException("Could not understand the persisted replication information", e);
				return null;
			}
		}

		private static async Task<JsonDocument> Load(string serverHash)
		{
			var path = "RavenDB Replication Information For - " + serverHash;

			StorageFile file;
			try
			{
				file = await folder.GetFileAsync(path);
			}
			catch (FileNotFoundException)
			{
				return null;
			}

			using (var stream = await file.OpenSequentialReadAsync())
			{
				return stream.AsStreamForRead().ToJObject().ToJsonDocument();
			}
		}

		public static void TrySavingReplicationInformationToLocalCache(string serverHash, JsonDocument document)
		{
			try
			{
				Save(serverHash, document).Wait();
			}
			catch (Exception e)
			{
				log.ErrorException("Could not persist the replication information", e);
			}
		}

		private static async Task Save(string serverHash, JsonDocument document)
		{
			var path = "RavenDB Replication Information For - " + serverHash;

			var file = await folder.CreateFileAsync(path, CreationCollisionOption.ReplaceExisting);
			using (var stream = await file.OpenStreamForWriteAsync())
			{
				document.ToJson().WriteTo(stream);
			}
		}
	}
}
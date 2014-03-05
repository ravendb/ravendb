#if!NETFX_CORE
using System;
using System.IO;
using System.IO.IsolatedStorage;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;

namespace Raven.Client.Connection
{
	public static class ReplicationInformerLocalCache
	{
		private readonly static ILog log = LogManager.GetCurrentClassLogger();

		public static IsolatedStorageFile GetIsolatedStorageFileForReplicationInformation()
		{
#if MONO
			return IsolatedStorageFile.GetUserStoreForApplication();
#else
			return IsolatedStorageFile.GetMachineStoreForDomain();
#endif
		}

		public static JsonDocument TryLoadReplicationInformationFromLocalCache(string serverHash)
		{
			try
			{
				using (var machineStoreForApplication = GetIsolatedStorageFileForReplicationInformation())
				{
					var path = "RavenDB Replication Information For - " + serverHash;

					if (machineStoreForApplication.GetFileNames(path).Length == 0)
						return null;

					using (var stream = new IsolatedStorageFileStream(path, FileMode.Open, machineStoreForApplication))
					{
						return stream.ToJObject().ToJsonDocument();
					}
				}
			}
			catch (Exception e)
			{
				log.ErrorException("Could not understand the persisted replication information", e);
				return null;
			}
		}

		public static void TrySavingReplicationInformationToLocalCache(string serverHash, JsonDocument document)
		{
			try
			{
				using (var machineStoreForApplication = GetIsolatedStorageFileForReplicationInformation())
				{
					var path = "RavenDB Replication Information For - " + serverHash;
					using (var stream = new IsolatedStorageFileStream(path, FileMode.Create, machineStoreForApplication))
					{
						document.ToJson().WriteTo(stream);
					}
				}
			}
			catch (Exception e)
			{
				log.ErrorException("Could not persist the replication information", e);
			}
		}
	}
}
#endif
#if!NETFX_CORE
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.IsolatedStorage;
using System.Text;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.RavenFS.Connections
{
	public static class ReplicationInformerLocalCache
	{
		private readonly static ILog Log = LogManager.GetCurrentClassLogger();

		public static IsolatedStorageFile GetIsolatedStorageFileForReplicationInformation()
		{
#if SILVERLIGHT
			return IsolatedStorageFile.GetUserStoreForSite();
#elif MONO
			return IsolatedStorageFile.GetUserStoreForApplication();
#else
			return IsolatedStorageFile.GetMachineStoreForDomain();
#endif
		}

        public static List<SynchronizationDestination> TryLoadReplicationInformationFromLocalCache(string serverHash)
		{
			try
			{
				using (var machineStoreForApplication = GetIsolatedStorageFileForReplicationInformation())
				{
					var path = "RavenFS Replication Information For - " + serverHash;

					if (machineStoreForApplication.GetFileNames(path).Length == 0)
						return null;

					using (var stream = new IsolatedStorageFileStream(path, FileMode.Open, machineStoreForApplication))
					{
						var buffer = new byte[stream.Length];
						var bytes = stream.Read(buffer, 0, (int)stream.Length);
					    Console.WriteLine(bytes);
					    return JsonConvert.DeserializeObject<List<SynchronizationDestination>>(Encoding.UTF8.GetString(buffer, 0, bytes));
					}
				}
			}
			catch (Exception e)
			{
				Log.ErrorException("Could not understand the persisted replication information", e);
				return null;
			}
		}

        public static void TrySavingReplicationInformationToLocalCache(string serverHash, List<SynchronizationDestination> destinations)
		{
			try
			{
				using (var machineStoreForApplication = GetIsolatedStorageFileForReplicationInformation())
				{
					var path = "RavenFS Replication Information For - " + serverHash;
					using (var stream = new IsolatedStorageFileStream(path, FileMode.Create, machineStoreForApplication))
					{
						var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(destinations));
						stream.Write(bytes, 0, bytes.Length);
					}
				}
			}
			catch (Exception e)
			{
				Log.ErrorException("Could not persist the replication information", e);
			}
		}
	}
}
#endif
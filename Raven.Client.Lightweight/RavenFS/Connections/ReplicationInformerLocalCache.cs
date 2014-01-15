#if!NETFX_CORE
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.IsolatedStorage;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;

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

		public static List<string> TryLoadReplicationInformationFromLocalCache(string serverHash)
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
						return Encoding.UTF8.GetString(buffer, 0, bytes).Split(' ').ToList();
					}
				}
			}
			catch (Exception e)
			{
				Log.ErrorException("Could not understand the persisted replication information", e);
				return null;
			}
		}

		public static void TrySavingReplicationInformationToLocalCache(string serverHash, List<string> destinations)
		{
			try
			{
				using (var machineStoreForApplication = GetIsolatedStorageFileForReplicationInformation())
				{
					var path = "RavenFS Replication Information For - " + serverHash;
					using (var stream = new IsolatedStorageFileStream(path, FileMode.Create, machineStoreForApplication))
					{
						var data = string.Join(" ", destinations);
						var bytes = Encoding.UTF8.GetBytes(data);
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
using System;
using System.Collections.Generic;
using System.IO;
#if !DNXCORE50
using System.IO.IsolatedStorage;
#endif
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
    public static class ReplicationInformerLocalCache
    {
#if !DNXCORE50
        private readonly static ILog Log = LogManager.GetCurrentClassLogger();
#else
        private readonly static ILog Log = LogManager.GetLogger(typeof(ReplicationInformerLocalCache));
#endif

#if !DNXCORE50
        public static IsolatedStorageFile GetIsolatedStorageFile()
        {
#if MONO
            return IsolatedStorageFile.GetUserStoreForApplication();
#else
            return IsolatedStorageFile.GetMachineStoreForDomain();
#endif
        }
#endif

        public static void ClearReplicationInformationFromLocalCache(string serverHash)
        {
#if !DNXCORE50
            try
            {
                using (var machineStoreForApplication = GetIsolatedStorageFile())
                {
                    var path = "RavenDB 3.5 Replication Information For - " + serverHash;

                    if (machineStoreForApplication.GetFileNames(path).Length == 0)
                        return;

                    machineStoreForApplication.DeleteFile(path);
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not clear the persisted replication information", e);
            }
#endif
        }

        public static JsonDocument TryLoadReplicationInformationFromLocalCache(string serverHash)
        {
#if !DNXCORE50
            try
            {
                using (var machineStoreForApplication = GetIsolatedStorageFile())
                {
                    var path = "RavenDB 3.5 Replication Information For - " + serverHash;

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
                Log.ErrorException("Could not understand the persisted replication information", e);
                return null;
            }
#else
            return null;
#endif
        }

        public static void TrySavingReplicationInformationToLocalCache(string serverHash, JsonDocument document)
        {
#if !DNXCORE50
            try
            {
                using (var machineStoreForApplication = GetIsolatedStorageFile())
                {
                    var path = "RavenDB 3.5 Replication Information For - " + serverHash;
                    using (var stream = new IsolatedStorageFileStream(path, FileMode.Create, machineStoreForApplication))
                    {
                        document.ToJson().WriteTo(stream);
                    }
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not persist the replication information", e);
            }
#endif
        }

        public static void ClearLocalCache(string serverHash)
        {
#if !DNXCORE50

            using (var machineStoreForApplication = GetIsolatedStorageFile())
            {
                var path = "RavenDB Cluster Nodes For - " + serverHash;
                if (machineStoreForApplication.GetFileNames(path).Length == 0)
                    return;

                try
                {
                    machineStoreForApplication.DeleteFile(path);
                }
                catch (Exception)
                {
                    // not important
                }
            }
#endif
        }
    }
}

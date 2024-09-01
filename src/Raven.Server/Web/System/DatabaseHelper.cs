using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;

namespace Raven.Server.Web.System
{
    public static class DatabaseHelper
    {
        private static readonly Lazy<string[]> ServerWideOnlyConfigurationKeys = new Lazy<string[]>(GetServerWideOnlyConfigurationKeys);

        public static string[] GetServerWideOnlyConfigurationKeys()
        {
            var keys = new string[0];
            foreach (var configurationProperty in typeof(RavenConfiguration).GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (configurationProperty.PropertyType.IsSubclassOf(typeof(ConfigurationCategory)) == false)
                    continue;

                foreach (var categoryProperty in configurationProperty.PropertyType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    foreach (var configurationEntryAttribute in categoryProperty.GetCustomAttributes<ConfigurationEntryAttribute>())
                    {
                        if (configurationEntryAttribute.Scope == ConfigurationEntryScope.ServerWideOrPerDatabase || configurationEntryAttribute.Scope == ConfigurationEntryScope.ServerWideOrPerDatabaseOrPerIndex)
                            continue;

                        Array.Resize(ref keys, keys.Length + 1);
                        keys[keys.Length - 1] = configurationEntryAttribute.Key;
                    }
                }
            }

            return keys;
        }

        public static void DeleteDatabaseFiles(RavenConfiguration configuration)
        {
            // we always want to try to delete the directories
            // because Voron and Periodic Backup are creating temp ones
            //if (configuration.Core.RunInMemory)
            //    return;

            IOExtensions.DeleteDirectory(configuration.Core.DataDirectory.FullPath);

            if (configuration.Storage.TempPath != null)
                IOExtensions.DeleteDirectory(configuration.Storage.TempPath.FullPath);

            if (configuration.Indexing.StoragePath != null)
                IOExtensions.DeleteDirectory(configuration.Indexing.StoragePath.FullPath);

            if (configuration.Indexing.TempPath != null)
                IOExtensions.DeleteDirectory(configuration.Indexing.TempPath.FullPath);
        }

        public static void Validate(string name, DatabaseRecord record, RavenConfiguration serverConfiguration)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            if (record.DatabaseName != null && string.Equals(name, record.DatabaseName, StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException("Name does not match.");

            if (record.Settings != null &&
                record.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Core.DataDirectory), out var dataDir) &&
                dataDir != null)
            {
                var databasePath = new PathSetting(dataDir, serverConfiguration.Core.DataDirectory.FullPath);

                if (databasePath.Equals(serverConfiguration.Core.DataDirectory))
                    throw new InvalidOperationException(
                        $"Forbidden data directory path for database '{name}': '{dataDir}'. This is the root path that RavenDB server uses to store data.");
                if (Path.GetPathRoot(databasePath.FullPath) == databasePath.FullPath)
                    throw new InvalidOperationException(
                        $"Forbidden data directory path for database '{name}': '{dataDir}'. You cannot use the root directory of the drive as the database path.");
            }

            foreach (var key in ServerWideOnlyConfigurationKeys.Value)
            {
                if (record.Settings != null && record.Settings.TryGetValue(key, out _))
                    throw new InvalidOperationException($"Detected '{key}' key in {nameof(DatabaseRecord.Settings)}. This is a server-wide configuration key and can only be set at server level.");
            }

            if (record.IsSharded)
            {
                if (record.Topology != null)
                {
                    throw new InvalidOperationException(
                        $"Problem trying to create a new sharded database {record.DatabaseName}."+
                        $" Sharded database can't have field {nameof(record.Topology)} defined in its record. Only the topologies inside {nameof(record.Sharding)} are relevant.");
                }
                
                if (record.Sharding?.Shards?.Count > 0)
                {
                    foreach (var (shardNumber, topology) in record.Sharding.Shards)
                    {
                        topology.ValidateTopology(ShardHelper.ToShardName(record.DatabaseName, shardNumber));
                    }
                }

                if (record.Sharding?.Orchestrator?.Topology?.Count > 0)
                {
                    record.Sharding.Orchestrator.Topology.ValidateTopology(record.DatabaseName);
                }
            }
            else
            {
                if (record.Sharding != null)
                    throw new InvalidOperationException(
                        $"Problem trying to create a new database {record.DatabaseName}. Can't have a sharding configuration in the record while no shards are defined.");
                
                record.Topology?.ValidateTopology(record.DatabaseName);
            }
        }

        public static void FillDatabaseTopology(ServerStore server, ClusterOperationContext context, string name, DatabaseRecord record, int replicationFactor,
            long? index, bool isRestore)
        {
            if (replicationFactor <= 0)
                throw new ArgumentException("Replication factor must be greater than 0.");

            try
            {
                Validate(name, record, server.Configuration);
            }
            catch (Exception e)
            {
                throw new BadRequestException("Database document validation failed.", e);
            }

            var clusterTopology = server.GetClusterTopology(context);
            ValidateClusterMembers(server, clusterTopology, record);
            InitializeDatabaseTopology(server, record, clusterTopology, replicationFactor);

            if (record.IsSharded)
            {
                server.Sharding.FillShardingConfiguration(record, clusterTopology, index, isRestore);

                if (string.IsNullOrEmpty(record.Sharding.DatabaseId))
                {
                    record.Sharding.DatabaseId = Guid.NewGuid().ToBase64Unpadded();
                    record.UnusedDatabaseIds ??= new HashSet<string>();
                    record.UnusedDatabaseIds.Add(record.Sharding.DatabaseId);
                }
            }
            else
            {
                if (record.Topology.Count == 0)
                {
                    server.AssignNodesToDatabase(clusterTopology,
                        record.DatabaseName,
                        record.Encrypted,
                        record.Topology);
                }
            }
        }

        private static void ValidateClusterMembers(ServerStore server, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
        {
            var topology = databaseRecord.Topology;

            if (topology == null)
                return;

            if (topology.Members?.Count == 1 && topology.Members[0] == "?")
            {
                // this is a special case where we pass '?' as member.
                topology.Members.Clear();
            }

            var unique = new HashSet<string>();
            foreach (var node in topology.AllNodes)
            {
                if (unique.Add(node) == false)
                    throw new InvalidOperationException($"node '{node}' already exists. This is not allowed. Database Topology : {topology}");

                var url = clusterTopology.GetUrlFromTag(node);
                if (databaseRecord.Encrypted && AdminDatabasesHandler.NotUsingHttps(url) && server.Server.AllowEncryptedDatabasesOverHttp == false)
                    throw new InvalidOperationException(
                        $"{databaseRecord.DatabaseName} is encrypted but node {node} with url {url} doesn't use HTTPS. This is not allowed.");
            }
        }

        private static void SetReplicationFactor(DatabaseTopology databaseTopology, ClusterTopology clusterTopology, int replicationFactor)
        {
            databaseTopology.ReplicationFactor = Math.Max(databaseTopology.ReplicationFactor, replicationFactor);
            databaseTopology.ReplicationFactor = Math.Min(databaseTopology.ReplicationFactor, clusterTopology.AllNodes.Count);
        }

        private static void InitializeDatabaseTopology(ServerStore server, DatabaseTopology databaseTopology, ClusterTopology clusterTopology, int replicationFactor,
            string clusterTransactionId)
        {
            Debug.Assert(databaseTopology != null);

            foreach (var node in databaseTopology.AllNodes)
            {
                if (string.IsNullOrEmpty(node))
                    throw new InvalidOperationException(
                        $"Attempting to save the database record but one of its specified topology nodes is null.");
            }

            if (databaseTopology.Members?.Count > 0)
            {
                foreach (var member in databaseTopology.Members)
                {
                    if (clusterTopology.Contains(member) == false)
                        throw new ArgumentException($"Failed to add node {member}, because we don't have it in the cluster.");
                }

                replicationFactor = databaseTopology.Count;
            }

            SetReplicationFactor(databaseTopology, clusterTopology, replicationFactor);

            databaseTopology.ClusterTransactionIdBase64 ??= clusterTransactionId;
            databaseTopology.DatabaseTopologyIdBase64 ??= Guid.NewGuid().ToBase64Unpadded();
            databaseTopology.Stamp ??= new LeaderStamp();
            databaseTopology.Stamp.Term = server.Engine.CurrentTerm;
            databaseTopology.Stamp.LeadersTicks = server.Engine.CurrentLeader?.LeaderShipDuration ?? 0;
            databaseTopology.NodesModifiedAt = SystemTime.UtcNow;
        }

        private static void InitializeDatabaseTopology(ServerStore server, DatabaseRecord databaseRecord, ClusterTopology clusterTopology, int replicationFactor)
        {
            var clusterTransactionId = Guid.NewGuid().ToBase64Unpadded();

            if (databaseRecord.IsSharded)
            {
                databaseRecord.Sharding.Orchestrator ??= new OrchestratorConfiguration();
                databaseRecord.Sharding.Orchestrator.Topology ??= new OrchestratorTopology();
                InitializeDatabaseTopology(server, databaseRecord.Sharding.Orchestrator.Topology, clusterTopology, replicationFactor, clusterTransactionId);

                foreach (var (shardNumber, databaseTopology) in databaseRecord.Sharding.Shards)
                    InitializeDatabaseTopology(server, databaseTopology, clusterTopology, replicationFactor, clusterTransactionId);
            }
            else
            {
                databaseRecord.Topology ??= new DatabaseTopology();
                InitializeDatabaseTopology(server, databaseRecord.Topology, clusterTopology, replicationFactor, clusterTransactionId);
            }
        }
    }
}

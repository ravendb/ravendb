using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;

namespace Raven.Server.ServerWide;

internal static class ConnectionStringConsumers
{
    public static List<(ConnectionStringType Type, string ConnectionStringName, TUsage Usage)> GetUsages<TUsage>(RawDatabaseRecord record, ConnectionStringType? type = null)
        where TUsage : ConnectionStringUsage, new()
    {
        var usages = new List<(ConnectionStringType, string, TUsage)>();

        if (Includes(ConnectionStringType.Raven))
        {
            foreach (var t in record.RavenEtls ?? [])
                Add(ConnectionStringType.Raven, t.ConnectionStringName, ConnectionStringUsageKind.RavenEtl, t.TaskId, null, t.Name);
            foreach (var t in record.ExternalReplications ?? [])
                Add(ConnectionStringType.Raven, t.ConnectionStringName, ConnectionStringUsageKind.ExternalReplication, t.TaskId, null, t.Name);
            foreach (var t in record.SinkPullReplications ?? [])
                Add(ConnectionStringType.Raven, t.ConnectionStringName, ConnectionStringUsageKind.PullReplicationAsSink, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Sql))
        {
            foreach (var t in record.SqlEtls ?? [])
                Add(ConnectionStringType.Sql, t.ConnectionStringName, ConnectionStringUsageKind.SqlEtl, t.TaskId, null, t.Name);
            foreach (var t in record.CdcSinks ?? [])
                Add(ConnectionStringType.Sql, t.ConnectionStringName, ConnectionStringUsageKind.CdcSink, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Olap))
        {
            foreach (var t in record.OlapEtls ?? [])
                Add(ConnectionStringType.Olap, t.ConnectionStringName, ConnectionStringUsageKind.OlapEtl, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.ElasticSearch))
        {
            foreach (var t in record.ElasticSearchEtls ?? [])
                Add(ConnectionStringType.ElasticSearch, t.ConnectionStringName, ConnectionStringUsageKind.ElasticSearchEtl, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Queue))
        {
            foreach (var t in record.QueueEtls ?? [])
                Add(ConnectionStringType.Queue, t.ConnectionStringName, ConnectionStringUsageKind.QueueEtl, t.TaskId, null, t.Name);
            foreach (var t in record.QueueSinks ?? [])
                Add(ConnectionStringType.Queue, t.ConnectionStringName, ConnectionStringUsageKind.QueueSink, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Snowflake))
        {
            foreach (var t in record.SnowflakeEtls ?? [])
                Add(ConnectionStringType.Snowflake, t.ConnectionStringName, ConnectionStringUsageKind.SnowflakeEtl, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Ai))
        {
            foreach (var t in record.EmbeddingsGenerations ?? [])
                Add(ConnectionStringType.Ai, t.ConnectionStringName, ConnectionStringUsageKind.EmbeddingsGeneration, t.TaskId, null, t.Name);
            foreach (var t in record.GenAis ?? [])
                Add(ConnectionStringType.Ai, t.ConnectionStringName, ConnectionStringUsageKind.GenAi, t.TaskId, null, t.Name);
            foreach (var a in record.AiAgents ?? [])
                Add(ConnectionStringType.Ai, a.ConnectionStringName, ConnectionStringUsageKind.AiAgent, null, a.Identifier, a.Name);
        }

        return usages;

        bool Includes(ConnectionStringType t) => type == null || type == t;

        void Add(ConnectionStringType t, string connectionStringName, ConnectionStringUsageKind kind, long? id, string identifier, string name)
        {
            if (connectionStringName == null)
                return;

            usages.Add((t, connectionStringName, new TUsage { Kind = kind, Id = id, Identifier = identifier, Name = name }));
        }
    }

    public static ConnectionStringUsage FindUsage(RawDatabaseRecord record, ConnectionStringType type, string connectionStringName)
    {
        foreach (var (_, name, usage) in GetUsages<ConnectionStringUsage>(record, type))
        {
            if (string.Equals(name, connectionStringName, StringComparison.Ordinal))
                return usage;
        }

        return null;
    }
}

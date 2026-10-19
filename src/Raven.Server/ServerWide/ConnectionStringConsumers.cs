using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;

namespace Raven.Server.ServerWide;

internal readonly record struct ConnectionStringUsageInfo(
    ConnectionStringType Type,
    string ConnectionStringName,
    ConnectionStringUsageKind Kind,
    long? Id,
    string Identifier,
    string Name);

internal static class ConnectionStringConsumers
{
    public static IEnumerable<ConnectionStringUsageInfo> GetUsages(RawDatabaseRecord record, ConnectionStringType? type = null)
    {
        if (Includes(ConnectionStringType.Raven))
        {
            foreach (var t in record.RavenEtls ?? [])
                yield return new(ConnectionStringType.Raven, t.ConnectionStringName, ConnectionStringUsageKind.RavenEtl, t.TaskId, null, t.Name);
            foreach (var t in record.ExternalReplications ?? [])
                yield return new(ConnectionStringType.Raven, t.ConnectionStringName, ConnectionStringUsageKind.ExternalReplication, t.TaskId, null, t.Name);
            foreach (var t in record.SinkPullReplications ?? [])
                yield return new(ConnectionStringType.Raven, t.ConnectionStringName, ConnectionStringUsageKind.PullReplicationAsSink, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Sql))
        {
            foreach (var t in record.SqlEtls ?? [])
                yield return new(ConnectionStringType.Sql, t.ConnectionStringName, ConnectionStringUsageKind.SqlEtl, t.TaskId, null, t.Name);
            foreach (var t in record.CdcSinks ?? [])
                yield return new(ConnectionStringType.Sql, t.ConnectionStringName, ConnectionStringUsageKind.CdcSink, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Olap))
        {
            foreach (var t in record.OlapEtls ?? [])
                yield return new(ConnectionStringType.Olap, t.ConnectionStringName, ConnectionStringUsageKind.OlapEtl, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.ElasticSearch))
        {
            foreach (var t in record.ElasticSearchEtls ?? [])
                yield return new(ConnectionStringType.ElasticSearch, t.ConnectionStringName, ConnectionStringUsageKind.ElasticSearchEtl, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Queue))
        {
            foreach (var t in record.QueueEtls ?? [])
                yield return new(ConnectionStringType.Queue, t.ConnectionStringName, ConnectionStringUsageKind.QueueEtl, t.TaskId, null, t.Name);
            foreach (var t in record.QueueSinks ?? [])
                yield return new(ConnectionStringType.Queue, t.ConnectionStringName, ConnectionStringUsageKind.QueueSink, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Snowflake))
        {
            foreach (var t in record.SnowflakeEtls ?? [])
                yield return new(ConnectionStringType.Snowflake, t.ConnectionStringName, ConnectionStringUsageKind.SnowflakeEtl, t.TaskId, null, t.Name);
        }

        if (Includes(ConnectionStringType.Ai))
        {
            foreach (var t in record.EmbeddingsGenerations ?? [])
                yield return new(ConnectionStringType.Ai, t.ConnectionStringName, ConnectionStringUsageKind.EmbeddingsGeneration, t.TaskId, null, t.Name);
            foreach (var t in record.GenAis ?? [])
                yield return new(ConnectionStringType.Ai, t.ConnectionStringName, ConnectionStringUsageKind.GenAi, t.TaskId, null, t.Name);
            foreach (var a in record.AiAgents ?? [])
                yield return new(ConnectionStringType.Ai, a.ConnectionStringName, ConnectionStringUsageKind.AiAgent, null, a.Identifier, a.Name);
        }

        bool Includes(ConnectionStringType t) => type == null || type == t;
    }

    public static ConnectionStringUsageInfo? FindUsage(RawDatabaseRecord record, ConnectionStringType type, string connectionStringName)
    {
        foreach (var usage in GetUsages(record, type))
        {
            if (string.Equals(usage.ConnectionStringName, connectionStringName, StringComparison.OrdinalIgnoreCase))
                return usage;
        }

        return null;
    }
}

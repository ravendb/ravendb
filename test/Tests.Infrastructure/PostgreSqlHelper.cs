using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Xunit;

namespace Tests.Infrastructure;

public static class PostgreSqlHelper
{
    public const string PgAdminSchemaTreeQuery = """
        SELECT
        nsp.oid,
        nsp.nspname as name,
        pg_catalog.has_schema_privilege(nsp.oid, 'CREATE') as can_create,
        pg_catalog.has_schema_privilege(nsp.oid, 'USAGE') as has_usage,
        des.description
        FROM
        pg_catalog.pg_namespace nsp
        LEFT OUTER JOIN pg_catalog.pg_description des ON
        (des.objoid=nsp.oid AND des.classoid='pg_namespace'::regclass)
        WHERE
        nspname NOT LIKE E'pg\\_%' AND
        NOT (
        (nsp.nspname = 'pg_catalog' AND EXISTS
        (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND
        relnamespace = nsp.oid LIMIT 1)) OR
        (nsp.nspname = 'pgagent' AND EXISTS
        (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND
        relnamespace = nsp.oid LIMIT 1)) OR
        (nsp.nspname = 'information_schema' AND EXISTS
        (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND
        relnamespace = nsp.oid LIMIT 1))
        )
        ORDER BY nspname
        """;

    public static string GetQueryString(PgQuery pgQuery)
    {
        return (string)typeof(PgQuery)
            .GetField("QueryString", BindingFlags.Instance | BindingFlags.NonPublic)
            ?.GetValue(pgQuery);
    }

    public static int? GetLimit(PgQuery pgQuery)
    {
        return (int?)typeof(RqlQuery)
            .GetField("_limit", BindingFlags.Instance | BindingFlags.NonPublic)
            ?.GetValue(pgQuery);
    }

    public static async Task<byte[]> ReadAllAsync(PipeReader reader, CancellationToken token)
    {
        var ms = new MemoryStream();
        while (true)
        {
            var result = await reader.ReadAsync(token);
            foreach (var segment in result.Buffer)
                ms.Write(segment.Span);
            reader.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted)
                break;
        }
        await reader.CompleteAsync();
        return ms.ToArray();
    }

    public static List<PgWireMessage> ParseMessages(byte[] buffer)
    {
        var messages = new List<PgWireMessage>();
        int i = 0;
        while (i + 5 <= buffer.Length)
        {
            var type = buffer[i];
            int length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(i + 1, 4));
            int payloadStart = i + 5;
            int payloadLength = length - 4;
            if (payloadLength < 0 || payloadStart + payloadLength > buffer.Length)
                break;

            messages.Add(new PgWireMessage((MessageType)type, new ReadOnlyMemory<byte>(buffer, payloadStart, payloadLength)));

            i = payloadStart + payloadLength;
        }

        return messages;
    }

    public static List<string[]> ParseDataRows(byte[] buffer)
    {
        var rows = new List<string[]>();
        foreach (var message in ParseMessages(buffer))
        {
            if (message.Type == MessageType.DataRow)
                rows.Add(message.AsDataRow());
        }

        return rows;
    }

    public static List<PgWireField> ParseRowDescription(byte[] buffer)
    {
        foreach (var message in ParseMessages(buffer))
        {
            if (message.Type == MessageType.RowDescription)
                return message.AsRowDescription();
        }

        return null;
    }

    public static string DecodeCell(PgTable table, int row, int column)
    {
        var cell = table.Data[row].ColumnData.Span[column];
        Assert.True(cell.HasValue);
        return Encoding.UTF8.GetString(cell.Value.Span);
    }
}

public readonly struct PgWireMessage(MessageType type, ReadOnlyMemory<byte> payload)
{
    public MessageType Type => type;

    public ReadOnlyMemory<byte> Payload => payload;

    public string[] AsDataRow()
    {
        var span = payload.Span;
        int pos = 0;
        int count = BinaryPrimitives.ReadInt16BigEndian(span.Slice(pos, 2));
        pos += 2;

        var values = new string[count];
        for (int c = 0; c < count; c++)
        {
            int size = BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4));
            pos += 4;
            if (size < 0)
                continue;
            values[c] = Encoding.UTF8.GetString(span.Slice(pos, size));
            pos += size;
        }

        return values;
    }

    public List<PgWireField> AsRowDescription()
    {
        var span = payload.Span;
        int pos = 0;
        int count = BinaryPrimitives.ReadInt16BigEndian(span.Slice(pos, 2));
        pos += 2;

        var fields = new List<PgWireField>(count);
        for (int f = 0; f < count; f++)
        {
            int terminator = span.Slice(pos).IndexOf((byte)0);
            var name = Encoding.UTF8.GetString(span.Slice(pos, terminator));
            pos += terminator + 1;

            fields.Add(new PgWireField(
                name,
                BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4)),
                BinaryPrimitives.ReadInt16BigEndian(span.Slice(pos + 4, 2)),
                BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos + 6, 4)),
                BinaryPrimitives.ReadInt16BigEndian(span.Slice(pos + 10, 2)),
                BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos + 12, 4)),
                BinaryPrimitives.ReadInt16BigEndian(span.Slice(pos + 16, 2))));

            pos += 18;
        }

        return fields;
    }

    public string AsCommandCompleteTag() => Encoding.ASCII.GetString(payload.Span).TrimEnd('\0');
}

public readonly record struct PgWireField(
    string Name,
    int TableOid,
    short ColumnAttributeNumber,
    int TypeOid,
    short TypeSize,
    int TypeModifier,
    short FormatCode);

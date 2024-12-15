using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries;

public sealed class StreamJsonlBlittableQueryResultWriter : IStreamQueryResultWriter<BlittableJsonReaderObject>
{
    private readonly AsyncBlittableJsonTextWriter _writer;

    public StreamJsonlBlittableQueryResultWriter(Stream stream, JsonOperationContext context)
    {
        _writer = new AsyncBlittableJsonTextWriter(context, stream);
    }


    public void StartResponse()
    {
    }

    public void StartResults()
    {
    }

    public void EndResults()
    {
    }

    public ValueTask AddResultAsync(BlittableJsonReaderObject res, CancellationToken token)
    {
        _writer.WriteStartObject();
        _writer.WritePropertyName("Item");
        _writer.WriteObject(res);
        _writer.WriteEndObject();

        _writer.WriteNewLine();
        return ValueTask.CompletedTask;
    }

    public void EndResponse()
    {
    }

    public ValueTask WriteErrorAsync(Exception e)
    {
        _writer.WriteStartObject();
        _writer.WritePropertyName("Error");
        _writer.WriteString(e.ToString());
        _writer.WriteEndObject();

        _writer.WriteNewLine();
        return ValueTask.CompletedTask;
    }

    public ValueTask WriteErrorAsync(string error)
    {
        _writer.WriteStartObject();
        _writer.WritePropertyName("Error");
        _writer.WriteString(error);
        _writer.WriteEndObject();

        _writer.WriteNewLine();
        return ValueTask.CompletedTask;
    }

    public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
    {
        _writer.WriteStartObject();
        _writer.WritePropertyName("Stats");
        _writer.WriteStartObject();

        _writer.WritePropertyName(nameof(StreamQueryStatistics.ResultEtag));
        _writer.WriteInteger(resultEtag);
        _writer.WriteComma();

        _writer.WritePropertyName(nameof(StreamQueryStatistics.IsStale));
        _writer.WriteBool(isStale);
        _writer.WriteComma();

        _writer.WritePropertyName(nameof(StreamQueryStatistics.IndexName));
        _writer.WriteString(indexName);
        _writer.WriteComma();

        _writer.WritePropertyName(nameof(StreamQueryStatistics.TotalResults));
        _writer.WriteInteger(totalResults);
        _writer.WriteComma();

        _writer.WritePropertyName(nameof(StreamQueryStatistics.IndexTimestamp));
        _writer.WriteString(timestamp.GetDefaultRavenFormat(isUtc: true));

        _writer.WriteEndObject();
        _writer.WriteEndObject();

        _writer.WriteNewLine();
    }

    public bool SupportStatistics => true;

    public ValueTask DisposeAsync()
    {
        return _writer.DisposeAsync();
    }
}

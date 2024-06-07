using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers;

public abstract class AbstractStreamJsonFileBlittableQueryResultWriter<T> : IStreamQueryResultWriter<T>
{
    protected readonly AsyncBlittableJsonTextWriter Writer;
    protected readonly string[] Properties;

    protected AbstractStreamJsonFileBlittableQueryResultWriter([NotNull] HttpResponse response, [NotNull] Stream stream, [NotNull] JsonOperationContext context, string[] properties = null, string jsonFileNamePrefix = "export")
    {
        if (response == null)
            throw new ArgumentNullException(nameof(response));
        if (stream == null)
            throw new ArgumentNullException(nameof(stream));
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        var encodedJsonFileName = Uri.EscapeDataString($"{jsonFileNamePrefix}_{SystemTime.UtcNow.ToString("yyyyMMdd_HHmm", CultureInfo.InvariantCulture)}.json");

        response.Headers[Constants.Headers.ContentDisposition] = $"attachment; filename=\"{encodedJsonFileName}\"; filename*=UTF-8''{encodedJsonFileName}";
        response.Headers[Constants.Headers.ContentType] = "text/json";

        Properties = properties;
        Writer = new AsyncBlittableJsonTextWriter(context, stream);
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        return Writer.DisposeAsync();
    }

    public void StartResponse()
    {
        Writer.WriteStartArray();
    }

    public void StartResults()
    {
    }

    public void EndResults()
    {
    }

    public abstract ValueTask AddResultAsync(T res, CancellationToken token);

    public void EndResponse()
    {
        Writer.WriteEndArray();
    }

    public ValueTask WriteErrorAsync(Exception e)
    {
        Writer.WriteString(e.ToString());
        return ValueTask.CompletedTask;
    }

    public ValueTask WriteErrorAsync(string error)
    {
        Writer.WriteString(error);
        return ValueTask.CompletedTask;
    }

    public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
    {
        throw new NotSupportedException();
    }

    public bool SupportStatistics => false;
}

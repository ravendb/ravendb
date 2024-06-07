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

public class StreamJsonFileDocumentQueryResultWriter : IStreamQueryResultWriter<Document>
{
    private readonly AsyncBlittableJsonTextWriter _writer;
    private readonly string[] _properties;

    private bool _first = true;

    public StreamJsonFileDocumentQueryResultWriter([NotNull] HttpResponse response, [NotNull] Stream stream, [NotNull] JsonOperationContext context, string[] properties = null, string jsonFileNamePrefix = "export")
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

        _properties = properties;
        _writer = new AsyncBlittableJsonTextWriter(context, stream);
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        return _writer.DisposeAsync();
    }

    public void StartResponse()
    {
        _writer.WriteStartArray();
    }

    public void StartResults()
    {
    }

    public void EndResults()
    {
    }

    public ValueTask AddResultAsync(Document res, CancellationToken token)
    {
        if (_first == false)
            _writer.WriteComma();
        else
            _first = false;

        if (_properties != null)
        {
            var innerFirst = true;
            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

            _writer.WriteStartObject();

            foreach (var property in _properties)
            {
                if (innerFirst == false)
                    _writer.WriteComma();
                else
                    innerFirst = false;

                if (Constants.Documents.Metadata.Id == property)
                {
                    _writer.WritePropertyName(Constants.Documents.Metadata.Id);
                    _writer.WriteString(res.Id.ToString(CultureInfo.InvariantCulture));
                }
                else
                {
                    var propertyIndex = res.Data.GetPropertyIndex(property);
                    if (propertyIndex == -1)
                        throw new InvalidOperationException();

                    res.Data.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                    _writer.WritePropertyName(propertyDetails.Name);
                    _writer.WriteValue(propertyDetails.Token & BlittableJsonReaderBase.TypesMask, propertyDetails.Value);
                }
            }

            _writer.WriteEndObject();

            return ValueTask.CompletedTask;
        }

        _writer.WriteObject(res.Data);
        return ValueTask.CompletedTask;
    }

    public void EndResponse()
    {
        _writer.WriteEndArray();
    }

    public ValueTask WriteErrorAsync(Exception e)
    {
        _writer.WriteString(e.ToString());
        return ValueTask.CompletedTask;
    }

    public ValueTask WriteErrorAsync(string error)
    {
        _writer.WriteString(error);
        return ValueTask.CompletedTask;
    }

    public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
    {
        throw new NotSupportedException();
    }

    public bool SupportStatistics => false;
}

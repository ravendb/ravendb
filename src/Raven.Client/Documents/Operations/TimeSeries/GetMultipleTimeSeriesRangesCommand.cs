using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries;

internal sealed class GetMultipleTimeSeriesRangesCommand : RavenCommand<GetMultipleTimeSeriesRangesCommand.Response>
{
    private readonly DocumentConventions _conventions;
    private readonly int _start;
    private readonly int _pageSize;
    private readonly bool _returnFullResults;
    private readonly RequestBody _ranges;

    public GetMultipleTimeSeriesRangesCommand(DocumentConventions conventions, Dictionary<string, List<TimeSeriesRange>> ranges, int start = 0, int pageSize = int.MaxValue, bool returnFullResults = false)
    {
        _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        _start = start;
        _pageSize = pageSize;
        _returnFullResults = returnFullResults;
        _ranges = new RequestBody
        {
            RangesPerDocumentId = ranges
        };
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        var pathBuilder = new StringBuilder(node.Url);
        pathBuilder.Append("/databases/")
            .Append(node.Database)
            .Append("/timeseries/ranges");

        if (_start > 0)
        {
            pathBuilder.Append("&start=")
                .Append(_start);
        }

        if (_pageSize < int.MaxValue)
        {
            pathBuilder.Append("&pageSize=")
                .Append(_pageSize);
        }

        if (_returnFullResults)
        {
            pathBuilder.Append("&full=").Append(true);
        }

        url = pathBuilder.ToString();

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_ranges, ctx)).ConfigureAwait(false), _conventions)
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            return;

        base.Result = JsonDeserializationClient.TimeSeriesRangesResponse(response);
    }

    internal sealed class RequestBody
    {
        public Dictionary<string, List<TimeSeriesRange>> RangesPerDocumentId { get; set; }
    }

    internal sealed class Response
    {
        public List<TimeSeriesDetails> Results { get; set; }
    }
}


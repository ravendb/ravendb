using System;
using System.IO;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow;
using Sparrow.Json;

namespace SlowTests.SparrowTests;

public class DownloadLogsCommand : RavenCommand<byte[]>
{
    private readonly DateTime? _startDate;
    private readonly DateTime? _endDate;

    public DownloadLogsCommand(DateTime? startDate, DateTime? endDate)
    {
        _startDate = startDate;
        _endDate = endDate;
        ResponseType = RavenCommandResponseType.Raw;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        var pathBuilder = new StringBuilder(node.Url);

        pathBuilder.Append("/admin/logs/download");

        if (_startDate.HasValue || _endDate.HasValue)
            pathBuilder.Append('?');

        if (_startDate.HasValue)
        {
            pathBuilder.Append("from=");
            pathBuilder.Append(_startDate.Value.ToUniversalTime().ToString(DefaultFormat.DateTimeFormatsToWrite));

            if (_endDate.HasValue)
                pathBuilder.Append('&');
        }

        if (_endDate.HasValue)
        {
            pathBuilder.Append("to=");
            pathBuilder.Append(_endDate.Value.ToUniversalTime().ToString(DefaultFormat.DateTimeFormatsToWrite));
        }

        url = pathBuilder.ToString();
        return new HttpRequestMessage { Method = HttpMethod.Get };
    }

    public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
    {
        if (response == null)
            return;

        var ms = new MemoryStream();
        stream.CopyTo(ms);

        Result = ms.ToArray();
    }
}

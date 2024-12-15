using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries;

public sealed class ShardedStreamQueryCsvResult : StreamQueryResult<BlittableJsonReaderObject>
{
    private readonly JsonOperationContext _context;
    private readonly string _timeSeries;

    public override async ValueTask AddResultAsync(BlittableJsonReaderObject result, CancellationToken token)
    {
        if (HasAnyWrites() == false)
            StartResponseIfNeeded();

        using (result)
        {
            var writer = GetWriter();
            if (string.IsNullOrEmpty(_timeSeries) == false)
            {
                if (result.TryGet(_timeSeries, out BlittableJsonReaderArray arr))
                {
                    var djv = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Id] = result.GetMetadata().GetId()
                    };
                    var properties = result.GetPropertyNames();
                    foreach (var property in properties)
                    {
                        if (_timeSeries == property)
                            continue;

                        djv[property] = result[property];
                    }
                    foreach (BlittableJsonReaderObject entry in arr)
                    {
                        foreach (var property in entry.GetPropertyNames())
                        {
                            djv[property] = entry[property];
                        }

                        await writer.AddResultAsync(_context.ReadObject(djv, "ts->csv"), token).ConfigureAwait(false);
                    }
                }
            }
            else
            {
                await writer.AddResultAsync(result, token).ConfigureAwait(false);
            }
        }

        GetToken().Delay();
    }

    public ShardedStreamQueryCsvResult(JsonOperationContext context, HttpResponse response, IStreamQueryResultWriter<BlittableJsonReaderObject> writer, IndexQueryServerSide query, OperationCancelToken token) : base(response, writer, indexDefinitionRaftIndex: null, token)
    {
        if (response.HasStarted)
            throw new InvalidOperationException("You cannot start streaming because response has already started.");
            
        _context = context;

        var tsField = query.Metadata.SelectFields?.SingleOrDefault(f => f.Function?.StartsWith(Constants.TimeSeries.QueryFunction) == true);
        _timeSeries = tsField?.Alias ?? tsField?.Name.Value;
    }
}

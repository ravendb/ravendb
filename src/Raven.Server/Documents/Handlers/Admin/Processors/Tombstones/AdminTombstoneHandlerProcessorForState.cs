using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Tombstones;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Tombstones;

internal sealed class AdminTombstoneHandlerProcessorForState : AbstractAdminTombstoneHandlerProcessorForState<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminTombstoneHandlerProcessorForState([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var state = RequestHandler.Database.TombstoneCleaner.GetState(addInfoForDebug: true);
        var response = new GetTombstonesStateCommand.Response(state);

        using (RequestHandler.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(response.MinAllDocsEtag));
                writer.WriteInteger(response.MinAllDocsEtag);
                writer.WriteComma();

                writer.WritePropertyName(nameof(response.MinAllTimeSeriesEtag));
                writer.WriteInteger(response.MinAllTimeSeriesEtag);
                writer.WriteComma();

                writer.WritePropertyName(nameof(response.MinAllCountersEtag));
                writer.WriteInteger(response.MinAllCountersEtag);
                writer.WriteComma();

                writer.WriteArray(context, nameof(response.Results), response.Results, (w, _, v) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(v.Collection));
                    w.WriteString(v.Collection);
                    w.WriteComma();

                    w.WritePropertyName(nameof(v.Documents));
                    w.WriteStartObject();
                    w.WritePropertyName(nameof(v.Documents.Component));
                    w.WriteString(v.Documents.Component);
                    w.WriteComma();
                    w.WritePropertyName(nameof(v.Documents.Etag));
                    w.WriteInteger(v.Documents.Etag);
                    w.WriteEndObject();
                    w.WriteComma();

                    w.WritePropertyName(nameof(v.TimeSeries));
                    w.WriteStartObject();
                    w.WritePropertyName(nameof(v.TimeSeries.Component));
                    w.WriteString(v.TimeSeries.Component);
                    w.WriteComma();
                    w.WritePropertyName(nameof(v.TimeSeries.Etag));
                    w.WriteInteger(v.TimeSeries.Etag);
                    w.WriteEndObject();
                    w.WriteComma();

                    w.WritePropertyName(nameof(v.Counters));
                    w.WriteStartObject();
                    w.WritePropertyName(nameof(v.Counters.Component));
                    w.WriteString(v.Counters.Component);
                    w.WriteComma();
                    w.WritePropertyName(nameof(v.Counters.Etag));
                    w.WriteInteger(v.Counters.Etag);
                    w.WriteEndObject();

                    w.WriteEndObject();
                });

                writer.WriteComma();

                writer.WritePropertyName(nameof(response.PerSubscriptionInfo));
                writer.WriteStartArray();
                if (response.PerSubscriptionInfo != null)
                {
                    var first = true;

                    foreach (var info in response.PerSubscriptionInfo)
                    {
                        if (first == false)
                            writer.WriteComma();

                        first = false;

                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(info.Identifier));
                        writer.WriteString(info.Identifier);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(info.Type));
                        writer.WriteString(info.Type.ToString());
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(info.Collection));
                        writer.WriteString(info.Collection);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(info.Etag));
                        writer.WriteInteger(info.Etag);

                        writer.WriteEndObject();
                    }
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetTombstonesStateCommand.Response> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}

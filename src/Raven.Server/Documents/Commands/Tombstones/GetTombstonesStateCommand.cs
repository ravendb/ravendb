using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Tombstones;

internal sealed class GetTombstonesStateCommand : RavenCommand<GetTombstonesStateCommand.Response>
{
    public sealed class Response
    {
        public Response()
        {
        }

        public Response([NotNull] TombstoneCleaner.TombstonesState state)
        {
            if (state == null)
                throw new ArgumentNullException(nameof(state));

            MinAllDocsEtag = state.MinAllDocsEtag;
            MinAllCountersEtag = state.MinAllCountersEtag;
            MinAllTimeSeriesEtag = state.MinAllTimeSeriesEtag;

            if (state.Tombstones != null)
            {
                Results = state.Tombstones
                    .Select(x => new TombstonesStateForCollection(x.Key, x.Value))
                    .ToList();
            }

            PerSubscriptionInfo = state.PerSubscriptionInfo;
        }

        public long MinAllDocsEtag { get; set; }

        public long MinAllTimeSeriesEtag { get; set; }

        public long MinAllCountersEtag { get; set; }

        public List<TombstonesStateForCollection> Results { get; set; }

        public List<TombstoneCleaner.TombstonesState.SubscriptionInfo> PerSubscriptionInfo { get; set; }

        public sealed class TombstonesStateForCollection
        {
            public TombstonesStateForCollection()
            {
            }

            public TombstonesStateForCollection([NotNull] string collection, [NotNull] TombstoneCleaner.StateHolder holder)
            {
                if (holder == null)
                    throw new ArgumentNullException(nameof(holder));

                Collection = collection ?? throw new ArgumentNullException(nameof(collection));

                Documents = holder.Documents;
                TimeSeries = holder.TimeSeries;
                Counters = holder.Counters;
            }

            public string Collection { get; set; }

            public TombstoneCleaner.State Documents { get; set; }

            public TombstoneCleaner.State TimeSeries { get; set; }

            public TombstoneCleaner.State Counters { get; set; }
        }
    }

    public GetTombstonesStateCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => false;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/admin/tombstones/state";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        Result = JsonDeserializationServer.GetTombstonesStateResponse(response);
    }
}

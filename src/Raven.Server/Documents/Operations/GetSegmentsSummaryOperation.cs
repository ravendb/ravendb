using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Operations
{
    internal class GetSegmentsSummaryOperation : IOperation<SegmentsSummary>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly DateTime? _from;
        private readonly DateTime? _to;

        public GetSegmentsSummaryOperation(string documentId, string name, DateTime? from, DateTime? to)
        {
            _documentId = documentId;
            _name = name;
            _from = from;
            _to = to;
        }
        public RavenCommand<SegmentsSummary> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetSegmentsSummaryCommand(_documentId, _name, _from, _to);
        }

        internal class GetSegmentsSummaryCommand : RavenCommand<SegmentsSummary>, IRaftCommand
        {
            private readonly string _documentId;
            private readonly string _name;
            private readonly DateTime? _from;
            private readonly DateTime? _to;

            public GetSegmentsSummaryCommand(string documentId, string name, DateTime? from, DateTime? to)
            {
                _documentId = documentId;
                _name = name;
                _from = from;
                _to = to;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url)
                    .Append($"/databases/{node.Database}/timeseries/debug/segments-summary")
                    .Append("?docId=")
                    .Append(_documentId)
                    .Append("&name=")
                    .Append(_name);

                if (_from.HasValue)
                    pathBuilder
                        .Append("&from=")
                        .Append(_from.Value.EnsureUtc().GetDefaultRavenFormat());

                if (_to.HasValue)
                    pathBuilder
                        .Append("&to=")
                        .Append(_to.Value.EnsureUtc().GetDefaultRavenFormat());

                url = pathBuilder.ToString();

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationServer.SegmentsSummary(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class SegmentsSummary
    {
        public List<TimeSeriesStorage.SegmentSummary> Results { get; set; }
    }
}

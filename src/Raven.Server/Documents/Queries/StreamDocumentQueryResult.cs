using System;
using Microsoft.AspNetCore.Http;
using Raven.Abstractions.Extensions;
using Raven.Client.Extensions;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamDocumentQueryResult : QueryResultServerSide, IDisposable
    {
        private readonly HttpResponse _response;
        private readonly BlittableJsonTextWriter _writer;
        private readonly JsonOperationContext _context;
        private bool _anyWrites;

        public StreamDocumentQueryResult(HttpResponse response, BlittableJsonTextWriter writer, JsonOperationContext context)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");

            _response = response;
            _writer = writer;
            _context = context;
        }

        public override void AddResult(Document result)
        {
            if (_anyWrites == false)
            {
                WriteHeaders();
                StartResponse();

                _anyWrites = true;
            }
            else
                _writer.WriteComma();

            using (result.Data)
            {
                _writer.WriteDocument(_context, result, metadataOnly: false);
            }
        }

        public override bool SupportsInclude => false;

        public void Dispose()
        {
            if (_anyWrites == false)
            {
                WriteHeaders();
                StartResponse();

                _anyWrites = true;
            }

            EndResponse();
        }

        private void WriteHeaders()
        {
            _response.Headers.Add("Raven-Result-Etag", ResultEtag.ToString());
            _response.Headers.Add("Raven-Is-Stale", IsStale ? "true" : "false");
            _response.Headers.Add("Raven-Index", IndexName);
            _response.Headers.Add("Raven-Total-Results", TotalResults.ToInvariantString());
            _response.Headers.Add("Raven-Index-Timestamp", IndexTimestamp.GetDefaultRavenFormat(isUtc: true));
        }

        private void StartResponse()
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Results");
            _writer.WriteStartArray();
        }

        private void EndResponse()
        {
            _writer.WriteEndArray();
            _writer.WriteEndObject();
        }
    }
}
using System;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Session;
using Raven.Server.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamDocumentQueryResult : QueryResultServerSide, IDisposable
    {
        private readonly BlittableJsonTextWriter _writer;
        private readonly JsonOperationContext _context;
        private bool _anyWrites;
        private bool _anyExceptions;

        public StreamDocumentQueryResult(HttpResponse response, BlittableJsonTextWriter writer, JsonOperationContext context)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");

            _writer = writer;
            _context = context;
        }

        public override void AddResult(Document result)
        {
            if (_anyWrites == false)
                StartResponseIfNeeded();
            else
                _writer.WriteComma();

            _writer.WriteDocument(_context, result, metadataOnly: false);
        }

        public override void HandleException(Exception e)
        {
            StartResponseIfNeeded();

            _anyExceptions = true;

            _writer.WriteEndArray();
            _writer.WriteComma();

            _writer.WritePropertyName("Error");
            _writer.WriteString(e.ToString());

            throw e;
        }

        private void StartResponseIfNeeded()
        {
            if (_anyWrites)
                return;

            StartResponse();

            _anyWrites = true;
        }

        public override bool SupportsExceptionHandling => true;

        public override bool SupportsInclude => false;

        public void Dispose()
        {
            StartResponseIfNeeded();

            EndResponse();
        }

        private void WriteStreamQueryStatistics()
        {
            _writer.WritePropertyName(nameof(StreamQueryStatistics.ResultEtag));
            _writer.WriteInteger(ResultEtag);
            _writer.WriteComma();

            _writer.WritePropertyName(nameof(StreamQueryStatistics.IsStale));
            _writer.WriteBool(IsStale);
            _writer.WriteComma();

            _writer.WritePropertyName(nameof(StreamQueryStatistics.IndexName));
            _writer.WriteString(IndexName);
            _writer.WriteComma();

            _writer.WritePropertyName(nameof(StreamQueryStatistics.TotalResults));
            _writer.WriteInteger(TotalResults);
            _writer.WriteComma();

            _writer.WritePropertyName(nameof(StreamQueryStatistics.IndexTimestamp));
            _writer.WriteString(IndexTimestamp.GetDefaultRavenFormat(isUtc: true));
            _writer.WriteComma();
        }

        private void StartResponse()
        {
            _writer.WriteStartObject();

            WriteStreamQueryStatistics();

            _writer.WritePropertyName("Results");
            _writer.WriteStartArray();
        }

        private void EndResponse()
        {
            if (_anyExceptions == false)
                _writer.WriteEndArray();

            _writer.WriteEndObject();
        }
    }
}

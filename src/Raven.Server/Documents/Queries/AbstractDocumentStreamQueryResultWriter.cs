using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public abstract class AbstractDocumentStreamQueryResultWriter<T> : IStreamQueryResultWriter<T>
    {
        public const string ErrorPropertyName = "Error";
        protected readonly AsyncBlittableJsonTextWriter Writer;
        protected readonly JsonOperationContext Context;

        protected AbstractDocumentStreamQueryResultWriter(Stream stream, JsonOperationContext context)
        {
            Context = context;
            Writer = new AsyncBlittableJsonTextWriter(context, stream);
        }

        public ValueTask DisposeAsync()
        {
            return Writer.DisposeAsync();
        }

        public void StartResponse()
        {
            Writer.WriteStartObject();
        }

        public void StartResults()
        {
            Writer.WritePropertyName("Results");
            Writer.WriteStartArray();
        }

        public void EndResults()
        {
            Writer.WriteEndArray();
        }

        public abstract ValueTask AddResultAsync(T res, CancellationToken token);

        public void EndResponse()
        {
            Writer.WriteEndObject();
        }

        public ValueTask WriteErrorAsync(Exception e)
        {
            Writer.WriteComma();

            Writer.WritePropertyName(ErrorPropertyName);
            Writer.WriteString(e.ToString());
            return default;
        }

        public ValueTask WriteErrorAsync(string error)
        {
            Writer.WritePropertyName(ErrorPropertyName);
            Writer.WriteString(error);
            return default;
        }

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            Writer.WritePropertyName(nameof(StreamQueryStatistics.ResultEtag));
            Writer.WriteInteger(resultEtag);
            Writer.WriteComma();

            Writer.WritePropertyName(nameof(StreamQueryStatistics.IsStale));
            Writer.WriteBool(isStale);
            Writer.WriteComma();

            Writer.WritePropertyName(nameof(StreamQueryStatistics.IndexName));
            Writer.WriteString(indexName);
            Writer.WriteComma();

            Writer.WritePropertyName(nameof(StreamQueryStatistics.TotalResults));
            Writer.WriteInteger(totalResults);
            Writer.WriteComma();

            Writer.WritePropertyName(nameof(StreamQueryStatistics.IndexTimestamp));
            Writer.WriteString(timestamp.GetDefaultRavenFormat(isUtc: true));
            Writer.WriteComma();
        }

        public bool SupportError => true;
        public bool SupportStatistics => true;
    }
}

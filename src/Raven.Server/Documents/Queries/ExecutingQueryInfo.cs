using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class ExecutingQueryInfo
    {
        public DateTime StartTime { get; }

        public string IndexName { get; set; }

        public IIndexQuery QueryInfo { get; }

        public long QueryId { get; }

        public bool IsStreaming { get; }

        public OperationCancelToken Token { get; }

        public long DurationInMs => _stopwatch.ElapsedMilliseconds;

        public TimeSpan Duration => _stopwatch.Elapsed;

        private readonly Stopwatch _stopwatch;

        public ExecutingQueryInfo(DateTime startTime, string indexName, IIndexQuery queryInfo, long queryId, bool isStreaming, OperationCancelToken token)
        {
            StartTime = startTime;
            IndexName = indexName;
            QueryInfo = queryInfo;
            QueryId = queryId;
            IsStreaming = isStreaming;
            _stopwatch = Stopwatch.StartNew();
            Token = token;
        }

        public void Write(AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(DurationInMs));
            writer.WriteDouble(DurationInMs);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Duration));
            writer.WriteString(Duration.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(IndexName));
            writer.WriteString(IndexName);
            writer.WriteComma();

            writer.WritePropertyName(nameof(QueryId));
            writer.WriteInteger(QueryId);
            writer.WriteComma();

            writer.WritePropertyName(nameof(StartTime));
            writer.WriteDateTime(StartTime, isUtc: true);
            writer.WriteComma();

            writer.WritePropertyName(nameof(QueryInfo));
            writer.WriteIndexQuery(context, QueryInfo);
            writer.WriteComma();

            writer.WritePropertyName(nameof(IsStreaming));
            writer.WriteBool(IsStreaming);

            writer.WriteEndObject();
        }
    }
}

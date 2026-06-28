using Raven.Client.Documents.Indexes;

namespace Raven.AiAppliance.Metrics;

/// <summary>
/// Map-reduce over the per-app <c>@conversations</c> collection, grouped by
/// (agent, UTC hour). One reduced row per agent per hour carries the
/// conversation count, message count, and summed token usage — the building
/// block for every windowed conversation/agent metric (last 24h / 7d / 30d and
/// the volume-over-time series). Hour granularity keeps a month of rows small
/// enough to sum client-side while still serving a 24-point hourly chart.
///
/// The map is a string definition (not the typed <see cref="AbstractIndexCreationTask{T}"/>):
/// the source collection is <c>@conversations</c>, which the RavenDB index LINQ
/// parser accepts as <c>docs.@conversations</c> even though it isn't a valid C#
/// identifier. The hour bucket is built from the date components (no reparse /
/// timezone conversion), so it is the stored UTC hour.
/// </summary>
internal sealed class ConversationMetricsIndex : AbstractIndexCreationTask
{
    internal sealed class Result
    {
        public string Agent { get; set; } = "";
        public DateTime Bucket { get; set; }
        public long Conversations { get; set; }
        public long Messages { get; set; }
        public long Tokens { get; set; }
    }

    public override string IndexName => "Conversations/Metrics";

    public override IndexDefinition CreateIndexDefinition()
    {
        return new IndexDefinition
        {
            Maps =
            {
                @"from c in docs.@conversations
                  select new {
                      Agent = c.Agent,
                      Bucket = new DateTime(c.CreatedAt.Year, c.CreatedAt.Month, c.CreatedAt.Day, c.CreatedAt.Hour, 0, 0, DateTimeKind.Utc),
                      Conversations = 1,
                      Messages = c.Messages.Count(m => m.role == ""user""),
                      Tokens = c.TotalUsage.TotalTokens
                  }"
            },
            Reduce =
                @"from r in results
                  group r by new { r.Agent, r.Bucket } into g
                  select new {
                      Agent = g.Key.Agent,
                      Bucket = g.Key.Bucket,
                      Conversations = g.Sum(x => x.Conversations),
                      Messages = g.Sum(x => x.Messages),
                      Tokens = g.Sum(x => x.Tokens)
                  }"
        };
    }
}

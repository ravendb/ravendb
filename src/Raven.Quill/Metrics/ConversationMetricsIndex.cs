using Raven.Client.Documents.Indexes;

namespace Raven.Quill.Metrics;

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
                      Messages = c.Messages.Count(m => m.role == ""user"" && (m.content == null || m.content.ToString().StartsWith(""AI Agent Parameters:"") == false)),
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

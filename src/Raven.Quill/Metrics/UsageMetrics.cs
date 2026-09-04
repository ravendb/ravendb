using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Quill.Channels;

namespace Raven.Quill.Metrics;

internal sealed class UsageMetrics
{
    internal const string IdPrefix = "metrics/usage/";

    internal const string Collection = "@UsageMetrics";

    internal const string SeriesName = "INC:Usage";

    internal const string DirectChannelKey = "direct";

    public string? Id { get; set; }

    public string Agent { get; set; } = "";

    public string ChannelId { get; set; } = "";

    public DateTime LastTurnAt { get; set; }

    internal static string IdFor(string agent, string channelId)
    {
        var channelKey = string.IsNullOrEmpty(channelId)
            ? DirectChannelKey
            : channelId.StartsWith(Channel.IdPrefix, StringComparison.Ordinal)
                ? channelId[Channel.IdPrefix.Length..]
                : channelId;
        return IdPrefix + Uri.EscapeDataString(agent) + "/" + Uri.EscapeDataString(channelKey);
    }

    internal static DateTime HourFloor(DateTime utc) =>
        new(utc.Year, utc.Month, utc.Day, utc.Hour, 0, 0, DateTimeKind.Utc);

    internal static async Task IncrementAsync(
        IAsyncDocumentSession session, string agent, string channelId, DateTime nowUtc,
        long conversations, long messages, long tokens, CancellationToken ct)
    {
        var id = IdFor(agent, channelId);
        var doc = await session.LoadAsync<UsageMetrics>(id, ct) ?? new UsageMetrics
        {
            Agent = agent,
            ChannelId = channelId,
        };

        doc.LastTurnAt = nowUtc;
        await session.StoreAsync(doc, id, ct);

        session.IncrementalTimeSeriesFor(id, SeriesName)
            .Increment(HourFloor(nowUtc), new double[] { conversations, messages, tokens });
    }
}

internal sealed class UsageIncrement
{
    [TimeSeriesValue(0)]
    public double Conversations { get; set; }

    [TimeSeriesValue(1)]
    public double Messages { get; set; }

    [TimeSeriesValue(2)]
    public double Tokens { get; set; }
}

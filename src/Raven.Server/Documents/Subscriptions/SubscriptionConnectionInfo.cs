using System;
using System.Collections.Generic;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Subscriptions;

public class SubscriptionConnectionInfo : IDynamicJson
{
    public string ClientUri { get; set; }
    public string Query { get; set; }
    public string LatestChangeVector { get; set; }
    public SubscriptionOpeningStrategy Strategy { get; set; }
    public DateTime Date { get; set; }
    public SubscriptionException ConnectionException { get; set; }
    public List<string> RecentSubscriptionStatuses { get; set; }
    public DynamicJsonValue TcpConnectionStats { get; set; }

    public SubscriptionConnectionStatsAggregator LastConnectionStats { get; set; }
    public List<SubscriptionBatchStatsAggregator> LastBatchesStats { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ClientUri)] = ClientUri,
            [nameof(Query)] = Query,
            [nameof(LatestChangeVector)] = LatestChangeVector,
            [nameof(Strategy)] = Strategy,
            [nameof(Date)] = Date,
            [nameof(ConnectionException)] = ConnectionException?.Message,
            [nameof(TcpConnectionStats)] = TcpConnectionStats,
            [nameof(RecentSubscriptionStatuses)] = new DynamicJsonArray(RecentSubscriptionStatuses == null ? Array.Empty<string>(): RecentSubscriptionStatuses)
        };
    }
}

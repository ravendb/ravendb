using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class TrafficWatchPayload : AbstractClusterDashboardNotification
    {
        internal List<TrafficWatchItem> TrafficPerDatabase { get; set; }

        public int RequestsPerSecond { get; set; }

        public int DocumentWritesPerSecond { get; set; }

        public int AttachmentWritesPerSecond { get; set; }

        public int CounterWritesPerSecond { get; set; }

        public int TimeSeriesWritesPerSecond { get; set; }

        public double DocumentsWriteBytesPerSecond { get; set; }

        public double AttachmentsWriteBytesPerSecond { get; set; }

        public double CountersWriteBytesPerSecond { get; set; }

        public double TimeSeriesWriteBytesPerSecond { get; set; }

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.Traffic;

        public override DynamicJsonValue ToJson()
        {
            var result = new TrafficWatchPayload();

            foreach (TrafficWatchItem item in TrafficPerDatabase)
            {
                result.Add(item);
            }

            return result.ToJsonInternal();
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var result = new TrafficWatchPayload();

            foreach (TrafficWatchItem item in TrafficPerDatabase)
            {
                if (filter(item.Database, requiresWrite: false))
                {
                    result.Add(item);
                }
            }

            return result.ToJsonInternal();
        }

        private DynamicJsonValue ToJsonInternal()
        {
            var json = base.ToJson();

            json[nameof(RequestsPerSecond)] = RequestsPerSecond;
            json[nameof(DocumentWritesPerSecond)] = DocumentWritesPerSecond;
            json[nameof(AttachmentWritesPerSecond)] = AttachmentWritesPerSecond;
            json[nameof(CounterWritesPerSecond)] = CounterWritesPerSecond;
            json[nameof(TimeSeriesWritesPerSecond)] = TimeSeriesWritesPerSecond;
            json[nameof(DocumentsWriteBytesPerSecond)] = DocumentsWriteBytesPerSecond;
            json[nameof(AttachmentsWriteBytesPerSecond)] = AttachmentsWriteBytesPerSecond;
            json[nameof(CountersWriteBytesPerSecond)] = CountersWriteBytesPerSecond;
            json[nameof(TimeSeriesWriteBytesPerSecond)] = TimeSeriesWriteBytesPerSecond;

            return json;
        }

        private void Add(TrafficWatchItem item)
        {
            RequestsPerSecond += item.RequestsPerSecond;
            AttachmentWritesPerSecond += item.AttachmentWritesPerSecond;
            AttachmentsWriteBytesPerSecond += item.AttachmentsWriteBytesPerSecond;
            CounterWritesPerSecond += item.CounterWritesPerSecond;
            CountersWriteBytesPerSecond += item.CountersWriteBytesPerSecond;
            DocumentWritesPerSecond += item.DocumentWritesPerSecond;
            DocumentsWriteBytesPerSecond += item.DocumentsWriteBytesPerSecond;
            TimeSeriesWritesPerSecond += item.TimeSeriesWritesPerSecond;
            TimeSeriesWriteBytesPerSecond += item.TimeSeriesWriteBytesPerSecond;
        }
    }
}

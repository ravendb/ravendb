using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class TrafficWatch : AbstractDashboardNotification
    {
        public List<TrafficWatchItem> Items { get; set; }

        public double AverageRequestDuration { get; set; }

        public TrafficWatch()
        {
            Items = new List<TrafficWatchItem>();
        }
    }

    public class TrafficWatchItem : IDynamicJson
    {
        public string Database { get; set; }

        public int RequestsPerSecond { get; set; }

        public double AverageRequestDuration { get; set; }

        public int DocumentWritesPerSecond { get; set; }

        public int AttachmentWritesPerSecond { get; set; }

        public int CounterWritesPerSecond { get; set; }

        public int TimeSeriesWritesPerSecond { get; set; }

        public double DocumentsWriteBytesPerSecond { get; set; }

        public double AttachmentsWriteBytesPerSecond { get; set; }

        public double CountersWriteBytesPerSecond { get; set; }

        public double TimeSeriesWriteBytesPerSecond { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(RequestsPerSecond)] = RequestsPerSecond,
                [nameof(DocumentWritesPerSecond)] = DocumentWritesPerSecond,
                [nameof(AttachmentWritesPerSecond)] = AttachmentWritesPerSecond,
                [nameof(CounterWritesPerSecond)] = CounterWritesPerSecond,
                [nameof(TimeSeriesWritesPerSecond)] = TimeSeriesWritesPerSecond,
                [nameof(DocumentsWriteBytesPerSecond)] = DocumentsWriteBytesPerSecond,
                [nameof(AttachmentsWriteBytesPerSecond)] = AttachmentsWriteBytesPerSecond,
                [nameof(CountersWriteBytesPerSecond)] = CountersWriteBytesPerSecond,
                [nameof(TimeSeriesWriteBytesPerSecond)] = TimeSeriesWriteBytesPerSecond,
                [nameof(AverageRequestDuration)] = AverageRequestDuration
            };
        }
    }
}

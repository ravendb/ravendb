using System;
using System.Text;

namespace Raven.NewClient.Abstractions.Data
{
    public class GCInfo
    {
        public DateTime LastForcedGCTime { get; set; }
        public long MemoryBeforeLastForcedGC { get; set; }
        public long MemoryAfterLastForcedGC { get; set; }

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append("{ LastForcedGCTime = ");
            builder.Append(LastForcedGCTime);
            builder.Append(", MemoryBeforeLastForcedGC = ");
            builder.Append(MemoryBeforeLastForcedGC);
            builder.Append(", MemoryAfterLastForcedGC = ");
            builder.Append(MemoryAfterLastForcedGC);
            builder.Append(" }");
            return builder.ToString();
        }
    }
}

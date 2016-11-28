using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Util.MiniMetrics
{
    /// <summary>
    /// The value reported by a Meter Tickable
    /// </summary>
    public struct MeterValue
    {
        public readonly long Count;
        public readonly double MeanRate;
        public readonly double OneMinuteRate;
        public readonly double FiveMinuteRate;
        public readonly double FifteenMinuteRate;
        

        [JsonConstructor]
        public MeterValue(long count, double meanRate, double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate)
        {
            this.Count = count;
            this.MeanRate = meanRate;
            this.OneMinuteRate = oneMinuteRate;
            this.FiveMinuteRate = fiveMinuteRate;
            this.FifteenMinuteRate = fifteenMinuteRate;
        }
    }
}

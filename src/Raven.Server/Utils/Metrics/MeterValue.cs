using Newtonsoft.Json;

namespace Raven.Server.Utils.Metrics
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
        public string Name { get; }

        [JsonConstructor]
        public MeterValue(string name, long count, double meanRate, double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate)
        {
            Count = count;
            MeanRate = meanRate;
            OneMinuteRate = oneMinuteRate;
            FiveMinuteRate = fiveMinuteRate;
            FifteenMinuteRate = fifteenMinuteRate;
            Name = name;
        }
    }
}

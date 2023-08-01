using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using static Sparrow.Server.Meters.IoMetrics;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class SlowIoDetails : INotificationDetails
    {
        public const int MaxNumberOfWrites = 500;

        public Dictionary<string, SlowWriteInfo> Writes { get; set; } = new Dictionary<string, SlowWriteInfo>();

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            
            var dict = new DynamicJsonValue();
            djv[nameof(Writes)] = dict;
            
            foreach (var key in Writes.Keys)
            {
                dict[key] = Writes[key].ToJson();
            }
            return djv;
        }

        public sealed class SlowWriteInfo : IDynamicJsonValueConvertible
        {
            public string Key => $"{Type}/{Path}";

            public MeterType Type { get; set; }

            public string Path { get; set; }

            public double DataWrittenInMb { get; set; }

            public double DurationInSec { get; set; }

            public double SpeedInMbPerSec { get; set; }

            public DateTime Date { get; set; }

            public SlowWriteInfo() { /* Used for deserialization */ }

            public SlowWriteInfo(IoChange ioChange, DateTime now)
            {
                DataWrittenInMb = ioChange.MeterItem.SizeInMb;
                Date = now;
                DurationInSec = ioChange.MeterItem.Duration.TotalSeconds;
                Path = ioChange.FileName;
                SpeedInMbPerSec = ioChange.MeterItem.RateOfWritesInMbPerSec;
                Type = ioChange.MeterItem.Type;
            }

            public void Update(IoChange ioChange, DateTime now)
            {
                DataWrittenInMb = ioChange.MeterItem.SizeInMb;
                Date = now;
                DurationInSec = ioChange.MeterItem.Duration.TotalSeconds;
                SpeedInMbPerSec = ioChange.MeterItem.RateOfWritesInMbPerSec;
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Path)] = Path,
                    [nameof(DataWrittenInMb)] = DataWrittenInMb,
                    [nameof(DurationInSec)] = DurationInSec,
                    [nameof(SpeedInMbPerSec)] = SpeedInMbPerSec,
                    [nameof(Date)] = Date,
                    [nameof(Type)] = Type
                };
            }
        }
    }
}

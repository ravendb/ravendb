using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class SlowWritesDetails : INotificationDetails
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

        public class SlowWriteInfo : IDynamicJsonValueConvertible
        {
            public string Path { get; set; }

            public double DataWrittenInMb { get; set; }

            public double DurationInSec { get; set; }

            public double SpeedInMbPerSec { get; set; }

            public DateTime Date { get; set; } 

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Path)] = Path,
                    [nameof(DataWrittenInMb)] = DataWrittenInMb,
                    [nameof(DurationInSec)] = DurationInSec,
                    [nameof(SpeedInMbPerSec)] = SpeedInMbPerSec,
                    [nameof(Date)] = Date
                };
            }
        }
    }
}

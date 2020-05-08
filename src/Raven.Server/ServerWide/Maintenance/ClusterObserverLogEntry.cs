using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ClusterObserverLogEntry : IDynamicJson
    {
        public DateTime Date { get; set; }
        public long Iteration { get ; set; }
        public string Database { get; set; }
        public string Message { get; set; }

        public override string ToString()
        {
            return $"{Date}: {nameof(Database)}:'{Database}', {nameof(Iteration)}:{Iteration}{Environment.NewLine}{Message}";
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
                {
                    [nameof(Date)] = Date,
                    [nameof(Iteration)] = Iteration,
                    [nameof(Database)] = Database,
                    [nameof(Message)] = Message
                };
        }
    }
}

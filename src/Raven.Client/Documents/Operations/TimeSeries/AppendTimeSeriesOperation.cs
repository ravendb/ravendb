using System;
using System.IO;
using System.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class AppendTimeSeriesOperation
    {
        public string Name;
        public DateTime Timestamp;
        public double[] Values;
        public string Tag;

        internal string ChangeVector;

        public static AppendTimeSeriesOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet(nameof(Name), out string name) == false || name == null)
                throw new InvalidDataException($"Missing '{nameof(Name)}' property");
            
            if (input.TryGet(nameof(Tag), out string tag) == false || name == null)
                throw new InvalidDataException($"Missing '{nameof(Tag)}' property");

            if (input.TryGet(nameof(Timestamp), out DateTime ts) == false || name == null)
                throw new InvalidDataException($"Missing '{nameof(Timestamp)}' property");
            
            if (input.TryGet(nameof(Values), out BlittableJsonReaderArray values) == false || name == null)
                throw new InvalidDataException($"Missing '{nameof(Values)}' property");

            var doubleValues = new double[values.Length];
            for (int i = 0; i < doubleValues.Length; i++)
            {
                doubleValues[i] = values.GetByIndex<double>(i);
            }

            var op = new AppendTimeSeriesOperation
            {
                Name = name,
                Timestamp = ts,
                Values = doubleValues,
                Tag = tag
            };

            return op;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Timestamp)] = Timestamp,
                [nameof(Values)] = new DynamicJsonArray(Values.Select(x=>(object)x)),
            };
        }
    }
}

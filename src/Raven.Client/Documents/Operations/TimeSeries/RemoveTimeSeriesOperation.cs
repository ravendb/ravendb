using System;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class RemoveTimeSeriesOperation
    {
        public string Name;
        public DateTime From, To;
        public static RemoveTimeSeriesOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet(nameof(Name), out string name) == false || name == null)
                throw new InvalidDataException($"Missing '{nameof(Name)}' property");

            if (input.TryGet(nameof(From), out DateTime from) == false || name == null)
                throw new InvalidDataException($"Missing '{nameof(From)}' property");
            
           
            if (input.TryGet(nameof(To), out DateTime to) == false || name == null)
                throw new InvalidDataException($"Missing '{nameof(To)}' property");


            var op = new RemoveTimeSeriesOperation
            {
                Name = name,
                From = from,
                To = to
            };

            return op;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(From)] = From,
                [nameof(To)] = To,
            };
        }
    }
}

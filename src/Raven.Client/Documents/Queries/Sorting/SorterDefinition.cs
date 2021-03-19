using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Queries.Sorting
{
    public class SorterDefinition
    {
        public string Name { get; set; }

        public string Code { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Code)] = Code
            };
        }

        internal void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
                throw new ArgumentException($"Sorter must have a '{nameof(Name)}' field.");

            if (string.IsNullOrWhiteSpace(Code))
                throw new ArgumentException($"Sorter must have a '{nameof(Code)}' field.");
        }
    }
}

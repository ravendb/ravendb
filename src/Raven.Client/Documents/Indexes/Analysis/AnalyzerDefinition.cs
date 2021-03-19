using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Indexes.Analysis
{
    public class AnalyzerDefinition
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
                throw new ArgumentException($"Analyzer must have a '{nameof(Name)}' field.");

            if (string.IsNullOrWhiteSpace(Code))
                throw new ArgumentException($"Analyzer must have a '{nameof(Code)}' field.");
        }
    }
}

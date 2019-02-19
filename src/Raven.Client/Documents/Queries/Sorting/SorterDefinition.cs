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
    }
}

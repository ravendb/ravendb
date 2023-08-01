using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide
{
    public sealed class CompactSettings
    {
        public string DatabaseName { get; set; }

        public bool Documents { get; set; }

        public string[] Indexes { get; set; }
        
        public bool SkipOptimizeIndexes { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(Documents)] = Documents,
                [nameof(Indexes)] = Indexes,
                [nameof(SkipOptimizeIndexes)] = SkipOptimizeIndexes
            };
        }
    }
}

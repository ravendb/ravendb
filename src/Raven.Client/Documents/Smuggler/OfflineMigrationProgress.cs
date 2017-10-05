using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler
{
    public class OfflineMigrationProgress : SmugglerResult.SmugglerProgress
    {
        public Counts DataExporter => ((OfflineMigrationResult)_result).DataExporter;

        public OfflineMigrationProgress(OfflineMigrationResult result) : base(result)
        {
            
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DataExporter)] = DataExporter.ToJson();
            return json;
        }
    }
}

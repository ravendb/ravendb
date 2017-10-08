using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler
{
    public class OfflineMigrationResult : SmugglerResult
    {
        public Counts DataExporter { get; set; }

        public OfflineMigrationResult()
        {
            _progress = new OfflineMigrationProgress(this);
            DataExporter = new Counts();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DataExporter)] = DataExporter.ToJson();
            return json;
        }
    }
}

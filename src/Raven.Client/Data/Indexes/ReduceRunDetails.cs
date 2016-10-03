using Sparrow.Json;

namespace Raven.Client.Data.Indexes
{
    public class ReduceRunDetails : IndexingPerformanceOperation.IDetails
    {
        public int NumberOfModifiedLeafs { get; set; }

        public int NumberOfModifiedBranches { get; set; }

        public void ToJson(BlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WritePropertyName(nameof(NumberOfModifiedLeafs));
            writer.WriteInteger(NumberOfModifiedLeafs);
            writer.WriteComma();

            writer.WritePropertyName(nameof(NumberOfModifiedBranches));
            writer.WriteInteger(NumberOfModifiedBranches);
            writer.WriteComma();
        }
    }
}

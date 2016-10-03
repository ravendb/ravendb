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

    public class MapRunDetails : IndexingPerformanceOperation.IDetails
    {
        public string BatchCompleteReason { get; set; }

        public long ProcessPrivateMemory { get; set; }

        public long ProcessWorkingSet { get; set; }

        public long CurrentlyAllocated { get; set; }

        public long AllocationBudget { get; set; }

        public void ToJson(BlittableJsonTextWriter writer, JsonOperationContext context)
        {
            if (BatchCompleteReason == null)
                return;

            writer.WritePropertyName(nameof(BatchCompleteReason));
            writer.WriteString(BatchCompleteReason);
            writer.WriteComma();

            if (ProcessPrivateMemory == 0)
                return;

            writer.WritePropertyName(nameof(ProcessPrivateMemory));
            writer.WriteInteger(ProcessPrivateMemory);
            writer.WriteComma();

            writer.WritePropertyName(nameof(ProcessWorkingSet));
            writer.WriteInteger(ProcessWorkingSet);
            writer.WriteComma();

            writer.WritePropertyName(nameof(CurrentlyAllocated));
            writer.WriteInteger(CurrentlyAllocated);
            writer.WriteComma();

            writer.WritePropertyName(nameof(AllocationBudget));
            writer.WriteInteger(AllocationBudget);
            writer.WriteComma();
        }
    }
}

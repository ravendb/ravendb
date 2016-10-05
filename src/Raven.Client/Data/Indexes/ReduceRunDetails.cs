namespace Raven.Client.Data.Indexes
{
    public class ReduceRunDetails : IndexingPerformanceOperation.IDetails
    {
        public int NumberOfModifiedLeafs { get; set; }

        public int NumberOfModifiedBranches { get; set; }
    }

    public class MapRunDetails : IndexingPerformanceOperation.IDetails
    {
        public string BatchCompleteReason { get; set; }

        public long ProcessPrivateMemory { get; set; }

        public long ProcessWorkingSet { get; set; }

        public long CurrentlyAllocated { get; set; }

        public long AllocationBudget { get; set; }
    }
}

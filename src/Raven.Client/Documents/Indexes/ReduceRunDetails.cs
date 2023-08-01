namespace Raven.Client.Documents.Indexes
{
    public sealed class ReduceRunDetails
    {
        public long ProcessPrivateMemory { get; set; }

        public long ProcessWorkingSet { get; set; }

        public long CurrentlyAllocated { get; set; }

        public long ReduceAttempts { get; set; }

        public long ReduceSuccesses { get; set; }

        public long ReduceErrors { get; set; }

        public TreesReduceDetails TreesReduceDetails { get; set; }
    }

    public sealed class TreesReduceDetails
    {
        public int NumberOfModifiedLeafs { get; set; }

        public int NumberOfModifiedBranches { get; set; }

        public int NumberOfCompressedLeafs { get; set; }
    }

    public sealed class MapRunDetails
    {
        public string BatchCompleteReason { get; set; }

        public long ProcessPrivateMemory { get; set; }

        public long ProcessWorkingSet { get; set; }

        public long CurrentlyAllocated { get; set; }

        public long AllocationBudget { get; set; }
    }

    public sealed class ReferenceRunDetails
    {
        public int ReferenceAttempts { get; set; }

        public int ReferenceSuccesses { get; set; }

        public int ReferenceErrors { get; set; }

        public long ProcessPrivateMemory { get; set; }

        public long ProcessWorkingSet { get; set; }

        public long CurrentlyAllocated { get; set; }

        public long AllocationBudget { get; set; }

        public string BatchCompleteReason { get; set; }
    }

    public sealed class CleanupRunDetails
    {
        public int DeleteSuccesses { get; set; }

        public string BatchCompleteReason { get; set; }
    }
}

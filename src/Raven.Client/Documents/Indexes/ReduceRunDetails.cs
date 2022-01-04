namespace Raven.Client.Documents.Indexes
{
    public class ReduceRunDetails
    {
        public long ProcessPrivateMemory { get; set; }

        public long ProcessWorkingSet { get; set; }

        public long CurrentlyAllocated { get; set; }

        public int ReduceAttempts { get; set; }

        public int ReduceSuccesses { get; set; }

        public int ReduceErrors { get; set; }

        public TreesReduceDetails TreesReduceDetails { get; set; }
    }

    public class TreesReduceDetails
    {
        public int NumberOfModifiedLeafs { get; set; }

        public int NumberOfModifiedBranches { get; set; }

        public int NumberOfCompressedLeafs { get; set; }
    }

    public class MapRunDetails
    {
        public string BatchCompleteReason { get; set; }

        public long ProcessPrivateMemory { get; set; }

        public long ProcessWorkingSet { get; set; }

        public long CurrentlyAllocated { get; set; }

        public long AllocationBudget { get; set; }
    }

    public class ReferenceRunDetails
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

    public class CleanupRunDetails
    {
        public int DeleteSuccesses { get; set; }

        public string BatchCompleteReason { get; set; }
    }
}

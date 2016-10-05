namespace Raven.Client.Data.Indexes
{
    public class ReduceRunDetails : IndexingPerformanceOperation.IDetails
    {
        public int NumberOfModifiedLeafs { get; set; }

        public int NumberOfModifiedBranches { get; set; }
    }
}

namespace Raven.Client.Documents.Indexes
{
    public sealed class IndexingStatus
    {
        public IndexRunningStatus Status { get; set; }

        public IndexStatus[] Indexes { get; set; }

        public sealed class IndexStatus
        {
            public string Name { get; set; }

            public IndexRunningStatus Status { get; set; }
        }
    }
}
namespace Raven.NewClient.Client.Data.Indexes
{
    public class IndexingStatus
    {
        public IndexRunningStatus Status { get; set; }

        public IndexStatus[] Indexes { get; set; }

        public class IndexStatus
        {
            public string Name { get; set; }

            public IndexRunningStatus Status { get; set; }
        }
    }
}
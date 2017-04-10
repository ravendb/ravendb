namespace Raven.Server.Documents.Studio
{
    public class FooterStatistics
    {
        public long CountOfDocuments { get; set; }

        public long CountOfIndexes { get; set; }

        public long CountOfStaleIndexes { get; set; }

        public long CountOfIndexingErrors { get; set; }
    }
}
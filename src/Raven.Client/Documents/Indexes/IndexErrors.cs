namespace Raven.Client.Documents.Indexes
{
    public sealed class IndexErrors
    {
        public IndexErrors()
        {
            Errors = new IndexingError[0];
        }

        public string Name { get; set; }
        
        public IndexingError[] Errors { get; set; }
    }
}
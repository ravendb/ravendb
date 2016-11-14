namespace Raven.NewClient.Client.Data.Indexes
{
    public class IndexErrors
    {
        public IndexErrors()
        {
            Errors = new IndexingError[0];
        }

        public string Name { get; set; }
        
        public IndexingError[] Errors { get; set; }
    }
}
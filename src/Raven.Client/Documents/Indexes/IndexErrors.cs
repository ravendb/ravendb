using System;

namespace Raven.Client.Documents.Indexes
{
    public class IndexErrors
    {
        public IndexErrors()
        {
            Errors = Array.Empty<IndexingError>();
        }

        public string Name { get; set; }
        
        public IndexingError[] Errors { get; set; }
    }
}

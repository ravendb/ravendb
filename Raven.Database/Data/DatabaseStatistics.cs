using System;

namespace Raven.Database.Data
{
    public class DatabaseStatistics
    {
        public int CountOfIndexes { get; set; }
        
        public int CountOfDocuments { get; set; }
        
        public string[] StaleIndexes { get; set; }
        
        public IndexStats[] Indexes { get; set; }
        
        public ServerError[] Errors { get; set; }
    }
}
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class StopWordsSetup
    {
        public string Id { get; set; }
        public List<string> StopWords { get; set; } 
    }
}

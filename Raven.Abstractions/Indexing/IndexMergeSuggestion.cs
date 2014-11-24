using System.Collections.Generic;

namespace Raven.Abstractions.Indexing
{
    public class IndexMergeSuggestion
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string With { get; set; }
        public List<string> Repl { get; set; }
    }
}
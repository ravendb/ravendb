using System.Collections.Generic;

namespace Raven.Client.Documents.Commands
{
    public class SuggestionResult
    {
        public string Name { get; set; }

        public List<string> Suggestions { get; set; }

        public SuggestionResult()
        {
            Suggestions = new List<string>();
        }
    }
}

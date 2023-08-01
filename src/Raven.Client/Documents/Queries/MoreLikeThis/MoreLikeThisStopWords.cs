using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public sealed class MoreLikeThisStopWords
    {
        public string Id { get; set; }
        public List<string> StopWords { get; set; } 
    }
}

using System.Collections.Generic;

namespace Raven.Client.MoreLikeThis
{
    public class StopWordsSetup
    {
        public string Id { get; set; }
        public List<string> StopWords { get; set; }
    }
}

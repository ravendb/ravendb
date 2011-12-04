using System.Collections.Generic;

#if !CLIENT
namespace Raven.Bundles.MoreLikeThis
#else
namespace Raven.Client.MoreLikeThis
#endif
{
    public class StopWordsSetup
    {
        public string Id { get; set; }
        public List<string> StopWords { get; set; } 
    }
}

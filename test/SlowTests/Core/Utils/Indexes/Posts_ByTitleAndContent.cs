#if !DNXCORE50
using Lucene.Net.Analysis;
#endif
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

using Post = SlowTests.Core.Utils.Entities.Post;

namespace SlowTests.Core.Utils.Indexes
{
    public class Posts_ByTitleAndContent : AbstractIndexCreationTask<Post>
    {
        public Posts_ByTitleAndContent()
        {
            Map = posts => from post in posts
                           select new
                           {
                               post.Title,
                               post.Desc
                           };

            Stores.Add(x => x.Title, FieldStorage.Yes);
            Stores.Add(x => x.Desc, FieldStorage.Yes);

#if !DNXCORE50
            Analyzers.Add(x => x.Title, typeof(SimpleAnalyzer).FullName);
            Analyzers.Add(x => x.Desc, typeof(SimpleAnalyzer).FullName);
#else
            Analyzers.Add(x => x.Title, "Lucene.Net.Analysis.SimpleAnalyzer");
            Analyzers.Add(x => x.Desc, "Lucene.Net.Analysis.SimpleAnalyzer");
#endif
        }
    }
}

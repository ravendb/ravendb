using Lucene.Net.Analysis;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using System.Linq;

namespace Raven.Tests.Core.Utils.Indexes
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

            Analyzers.Add(x => x.Title, typeof(SimpleAnalyzer).FullName);
            Analyzers.Add(x => x.Desc, typeof(SimpleAnalyzer).FullName);
        }
    }
}

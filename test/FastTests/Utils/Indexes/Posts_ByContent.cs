using System.Linq;
using Raven.Client.Documents.Indexes;
using Post = SlowTests.Core.Utils.Entities.Post;
using PostContent = SlowTests.Core.Utils.Entities.PostContent;

namespace SlowTests.Core.Utils.Indexes
{
    public class Posts_ByContent : AbstractIndexCreationTask<Post>
    {
        public Posts_ByContent()
        {
            Map = posts => from post in posts
                           let body = LoadDocument<PostContent>(post.Id + "/content")
                           select new
                           {
                               Text = body == null ? null : body.Text
                           };
        }
    }
}
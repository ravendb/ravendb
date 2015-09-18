using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Document
{

    public class PerCollectionEtag : RavenTest 
    {

        [Theory]
        [PropertyData("Storages")]
        public void CanRetreivePerCollectionETag(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Post { Id = "posts/1", Title = "test", Body = "etags"});
                    session.Store(new Comment { Id = "comments/1" ,Title = "test"});
                    session.Store(new { Id = "no-collection-name" ,Title = "test"});
                    session.SaveChanges();
                }

                var postsCollectioEtag = store.SystemDatabase.LastCollectionEtags.GetLastEtagForCollection("Posts");
                var commentsCollectionEtag = store.SystemDatabase.LastCollectionEtags.GetLastEtagForCollection("Comments");

                var postEtag = store.DatabaseCommands.Head("posts/1").Etag;
                var commentETag = store.DatabaseCommands.Head("comments/1").Etag;

                Assert.Equal(postEtag, postsCollectioEtag);
                Assert.Equal(commentETag, commentsCollectionEtag);
            }
        }

        public class Post
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string Body { get; set; }
        }

        public class Comment
        {
            public string Id { get; set; }
            public string Title { get; set; }
        }
    }
}

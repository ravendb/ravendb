using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13016 : RavenTestBase
    {
        public RavenDB_13016(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Can_project_on_sub_collection_with_Id_property(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/705-A");
                    session.Store(new PostComment
                    {
                        PostId = "posts/20785-A",

                        Comments = new List<Comment>
                        {
                            new Comment
                            {
                                Id = "fcdffb03-ea82-410c-b415-a6d5859a50f8",
                                OwnerId = "users/705-A",
                                Text = "test comment",
                                DatePosted = new DateTime(2019, 3, 4, 7, 26, 29, 45)
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<PostComment>()
                                select new PostCommentsAsPublic
                                {
                                    Comments = from comment in x.Comments
                                               let owner = RavenQuery.Load<User>(comment.OwnerId)
                                               select new PostCommentAsPublic
                                               {
                                                   Id = comment.Id,
                                                   Owner = new UserAsReference
                                                   {
                                                       Id = owner.Id,
                                                   }
                                               }
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
                        "from 'PostComments' as x select { " +
                            "Comments : ((((x?.Comments??[]).map(function(comment){return {comment:comment,owner:load(comment?.OwnerId)};}))" +
                                        "??[]).map(function(__rvn0){return {Id:id((__rvn0?.comment)),Owner:{Id:id((__rvn0?.owner))}};})) }"
                        , query.ToString());


                    var result = query.ToList();

                    Assert.Equal(1, result.Count);

                    var commentsList = result[0].Comments.ToList();

                    Assert.Equal("fcdffb03-ea82-410c-b415-a6d5859a50f8", commentsList[0].Id);
                    Assert.Equal("users/705-A", commentsList[0].Owner.Id);

                }
            }
        }

        private class User
        {
            public string Id { get; set; }
        }

        private class UserAsReference
        {
            public string Id { get; set; }
        }

        private class Comment
        {
            public string Id { get; set; }
            public string OwnerId { get; set; }
            public string Text { get; set; }
            public DateTime DatePosted { get; set; }
        }

        private class PostComment
        {
            public string PostId { get; set; }
            public List<Comment> Comments { get; set; }
        }

        private class PostCommentAsPublic
        {
            public string Id { get; set; }
            public UserAsReference Owner { get; set; }
        }

        private class PostCommentsAsPublic
        {
            public IEnumerable<PostCommentAsPublic> Comments { get; set; }
        }
    }
}

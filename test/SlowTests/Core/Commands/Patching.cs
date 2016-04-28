using System;
using System.Threading.Tasks;

using FastTests;

using Raven.Client.Data;
using Raven.Json.Linq;

using Xunit;

using Post = SlowTests.Core.Utils.Entities.Post;

namespace SlowTests.Core.Commands
{
    public class Patching : RavenTestBase
    {
        [Fact]
        public async Task CanDoScriptedPatching()
        {
            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    "posts/1",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 1",
                        Comments = new Post[] { }
                    }),
                    new RavenJObject());

                var comment = new Post
                {
                    Title = "comment 1"
                };

                store.DatabaseCommands.Patch(
                    "posts/1",
                    new PatchRequest
                    {
                        Script = @"this.Comments.push(comment1)",
                        Values = { { "comment1", comment } }
                    });
                var result = store.DatabaseCommands.Get("posts/1");
                var comments = result.DataAsJson.Value<RavenJArray>("Comments");
                Assert.Equal(1, comments.Length);
                Assert.Equal("comment 1", comments[0].Value<string>("Title"));


                store.DatabaseCommands.Put(
                    "posts/2",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 2",
                        AttachmentIds = new[] { "id1", "id2" }
                    }),
                    new RavenJObject());

                store.DatabaseCommands.Patch(
                    "posts/2",
                    new PatchRequest
                    {
                        Script = @"this.AttachmentIds.Remove(tagToRemove)",
                        Values = { { "tagToRemove", "id2" } }
                    });
                result = store.DatabaseCommands.Get("posts/2");
                Assert.Equal(1, result.DataAsJson.Value<RavenJArray>("AttachmentIds").Length);
                Assert.Equal("id1", result.DataAsJson.Value<RavenJArray>("AttachmentIds")[0]);


                store.DatabaseCommands.Patch(
                    "posts/1",
                    new PatchRequest
                    {
                        Script = @"
                            this.Comments.RemoveWhere(function(comment) {
                                return comment.Title === 'comment 1';
                            });",
                    });
                result = store.DatabaseCommands.Get("posts/1");
                comments = result.DataAsJson.Value<RavenJArray>("Comments");
                Assert.Equal(0, comments.Length);


                var comment1 = new Post
                {
                    Title = "Comment 1",
                    Desc = "Some post without searched phrase inside."
                };
                var comment2 = new Post
                {
                    Title = "Comment 2",
                    Desc = "Some post with Raven phrase inside."
                };

                store.DatabaseCommands.Put(
                    "posts/3",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 3",
                        Comments = new[] { comment1, comment2 }
                    }),
                    new RavenJObject());
                store.DatabaseCommands.Patch(
                    "posts/3",
                    new PatchRequest
                    {
                        Script = @"
                            this.Comments.Map(function(comment) {  
                                if(comment.Desc.indexOf(""Raven"") != -1)
                                {
                                    comment.Title = ""[Raven] "" + comment.Title;
                                }
                                return comment;
                            });
                        "
                    });
                result = store.DatabaseCommands.Get("posts/3");
                comments = result.DataAsJson.Value<RavenJArray>("Comments");
                Assert.Equal(2, comments.Length);
                Assert.Equal("Comment 1", comments[0].Value<string>("Title"));
                Assert.Equal("[Raven] Comment 2", comments[1].Value<string>("Title"));


                store.DatabaseCommands.Put(
                    "posts/4",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 4",
                        AttachmentIds = new[] { "posts/5" }
                    }),
                    new RavenJObject());
                store.DatabaseCommands.Put(
                    "posts/5",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 5"
                    }),
                    new RavenJObject());
                store.DatabaseCommands.Patch(
                    "posts/4",
                    new PatchRequest
                    {
                        Script = @"
                            var loaded = LoadDocument(this.AttachmentIds[0]);
                            this.Title = loaded.Title;
                        "
                    });
                result = store.DatabaseCommands.Get("posts/4");
                Assert.Equal("Post 5", result.DataAsJson.Value<string>("Title"));


                var output = store.DatabaseCommands.Patch(
                    "posts/4",
                    new PatchRequest
                    {
                        Script = @"
                            var loaded = LoadDocument(this.AttachmentIds[0]);
                            this.Title = loaded.Title;
                            output(this.Title); 
                        "
                    });
                var debugInfo = output.Value<RavenJArray>("Debug");
                Assert.Equal("Post 5", debugInfo[0]);

                store.DatabaseCommands.Patch(
                    "posts/4",
                    new PatchRequest
                    {
                        Script = @"
                            PutDocument('posts/4',
                                { 'Title' : 'new title' }
                            );"
                    });
                var post = store.DatabaseCommands.Get("posts/4");
                Assert.NotNull(post);
                Assert.Equal("new title", post.DataAsJson.Value<string>("Title"));
            }
        }

        [Fact(Skip = "Missing feature: Patching - the ability to generate document Ids during PutDocument")]
        public async Task CanGenerteDynamicIdsOnPutDocument()
        {
            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    "posts/1",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 1",
                        Comments = new Post[] { }
                    }),
                    new RavenJObject());
                var output = store.DatabaseCommands.Patch(
                    "posts/1",
                    new PatchRequest
                    {
                        Script = @"
                            var postId = PutDocument('posts/',
                                { 'Title' : 'unknown post id' }
                            );
                            this.Title = postId;
                            output(postId);"


                    });
                using (var session = store.OpenSession())
                {
                    var debugInfo = output.Value<RavenJArray>("Debug");
                    var postId = debugInfo[0].ToString();
                    var post = session.Load<Post>("posts/1");
                    Assert.Equal(postId, post.Title);
                }
                output = store.DatabaseCommands.Patch(
                    "posts/1",
                    new PatchRequest
                    {
                        Script = @"
                            var postId = PutDocument(null,
                                { 'Title' : 'unknown post id' }
                            );
                            this.Title = postId;
                            output(postId);"
                    });
                using (var session = store.OpenSession())
                {
                    var debugInfo = output.Value<RavenJArray>("Debug");
                    var postId = debugInfo[0].ToString();
                    var post = session.Load<Post>("posts/1");
                    Assert.Equal(postId, post.Title);
                    Guid id;
                    Assert.True(Guid.TryParse(postId, out id));
                }
            }
        }
    }
}

using System;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Operations;
using Sparrow.Json;
using Xunit;
using Post = SlowTests.Core.Utils.Entities.Post;
using Xunit.Abstractions;

namespace SlowTests.Core.Commands
{
    public class Patching : RavenTestBase
    {
        public Patching(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanDoScriptedPatching(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    commands.Put("posts/1", null, new Post
                    {
                        Title = "Post 1",
                        Comments = new Post[] { }
                    }, null);

                    var comment = new Post
                    {
                        Title = "comment 1"
                    };

                    store.Operations.Send(new PatchOperation("posts/1", null,
                        new PatchRequest
                        {
                            Script = @"this.Comments.push(args.comment1)",
                            Values = { { "comment1", comment } }
                        }));

                    dynamic result = commands.Get("posts/1");
                    dynamic comments = result.Comments;
                    Assert.Equal(1, comments.Length);
                    Assert.Equal("comment 1", comments[0].Title.ToString());

                    commands.Put("posts/2", null, new Post
                    {
                        Title = "Post 2",
                        AttachmentIds = new[] { "id1", "id2" }
                    }, null);

                    store.Operations.Send(new PatchOperation("posts/2", null,
                        new PatchRequest
                        {
                            Script = @"
this.AttachmentIds = this.AttachmentIds.filter(function (t) { 
    return t != args.tagToRemove
})",
                            Values = { { "tagToRemove", "id2" } }
                        }));

                    result = commands.Get("posts/2");
                    var attachmentIds = result.AttachmentIds;
                    Assert.Equal(1, attachmentIds.Length);
                    Assert.Equal("id1", attachmentIds[0]);

                    store.Operations.Send(new PatchOperation("posts/1", null,
                        new PatchRequest
                        {
                            Script = @"
this.Comments = this.Comments.filter(function (c) { 
    return c.Title !== 'comment 1'
});",
                        }));

                    result = commands.Get("posts/1");
                    comments = result.Comments;
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

                    commands.Put("posts/3", null, new Post
                    {
                        Title = "Post 3",
                        Comments = new[] { comment1, comment2 }
                    }, null);

                    store.Operations.Send(new PatchOperation("posts/3", null,
                        new PatchRequest
                        {
                            Script = @"
                            this.Comments.map(function(comment) {  
                                if(comment.Desc.indexOf(""Raven"") != -1)
                                {
                                    comment.Title = ""[Raven] "" + comment.Title;
                                }
                                return comment;
                            });
                        "
                        }));

                    result = commands.Get("posts/3");
                    comments = result.Comments;
                    Assert.Equal(2, comments.Length);
                    Assert.Equal("Comment 1", comments[0].Title.ToString());
                    Assert.Equal("[Raven] Comment 2", comments[1].Title.ToString());

                    commands.Put("posts/4", null, new Post
                    {
                        Title = "Post 4",
                        AttachmentIds = new[] { "posts/5" }
                    }, null);

                    commands.Put("posts/5", null, new Post
                    {
                        Title = "Post 5"
                    }, null);

                    store.Operations.Send(new PatchOperation("posts/4", null,
                        new PatchRequest
                        {
                            Script = @"
                            var loaded = load(this.AttachmentIds[0]);
                            this.Title = loaded.Title;
                        "
                        }));

                    result = commands.Get("posts/4"); ;
                    Assert.Equal("Post 5", result.Title.ToString());

                    var command = new PatchOperation.PatchCommand(
                        commands.Context,
                        "posts/4",
                        null,
                        new PatchRequest
                        {
                            Script = @"
                            var loaded = load(this.AttachmentIds[0]);
                            this.Title = loaded.Title;
                            output(this.Title); 
                        "
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: true,
                        test: false);

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var commandResult = command.Result;
                    var array = (BlittableJsonReaderArray)commandResult.Debug["Info"];
                    var id = array[0].ToString();
                    Assert.Equal("Post 5", id);

                    store.Operations.Send(new PatchOperation("posts/3", null,
                       new PatchRequest
                       {
                           Script = @"
                            put('posts/4',
                                { 'Title' : 'new title' }
                            );"
                       }));

                    result = commands.Get("posts/4");
                    Assert.NotNull(result);
                    Assert.Equal("new title", result.Title.ToString());
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanGenerateDynamicIdsOnPutDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    commands.Put("posts/1", null, new Post
                    {
                        Title = "Post 1",
                        Comments = new Post[] { }
                    }, null);

                    var command = new PatchOperation.PatchCommand(
                        commands.Context,
                        "posts/1",
                        null,
                        new PatchRequest
                        {
                            Script = @"
                            var postId = put('posts/',
                                { 'Title' : 'unknown post id' }
                            );
                            this.Title = postId;
                            output(postId);"
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: true,
                        test: false);

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var result = command.Result;
                    var array = (BlittableJsonReaderArray)result.Debug["Info"];

                    using (var session = store.OpenSession())
                    {
                        var postId = array[0].ToString();
                        var post = session.Load<Post>("posts/1");
                        Assert.Equal(postId, post.Title);
                    }

                    command = new PatchOperation.PatchCommand(
                        commands.Context,
                        "posts/1",
                        null,
                        new PatchRequest
                        {
                            Script = @"
                            var postId = put(null,
                                { 'Title' : 'unknown post id' }
                            );
                            this.Title = postId;
                            output(postId);"
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: true,
                        test: false);

                    commands.RequestExecutor.Execute(command, commands.Context);

                    result = command.Result;
                    array = (BlittableJsonReaderArray)result.Debug["Info"];

                    using (var session = store.OpenSession())
                    {
                        var postId = array[0].ToString();
                        var post = session.Load<Post>("posts/1");
                        Assert.Equal(postId, post.Title);
                        Guid id;
                        Assert.True(Guid.TryParse(postId, out id));
                    }
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanTestPatches(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Title = "Post 1",
                        Comments = new Post[] { }
                    });

                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var command = new PatchOperation.PatchCommand(
                        commands.Context,
                        "posts/1-A",
                        null,
                        new PatchRequest
                        {
                            Script = @"this.Title = 'abcd'",
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: false,
                        test: true);

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var modified = (Post)store.Conventions.Serialization.DeserializeEntityFromBlittable(typeof(Post), command.Result.ModifiedDocument);

                    Assert.Equal("abcd", modified.Title);
                }

                using (var session = store.OpenSession())
                {
                    var post = session.Load<Post>("posts/1-A");

                    Assert.Equal("Post 1", post.Title);
                }

                using (var commands = store.Commands())
                {
                    var command = new PatchOperation.PatchCommand(
                        commands.Context,
                        "posts/1-A",
                        null,
                        new PatchRequest
                        {
                            Script = @"
                            var postId = put('posts/',
                                { 'Title' : 'unknown post id' }
                            );
                            this.Title = postId;
                            output(postId);"
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: false,
                        test: true);

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var array = (BlittableJsonReaderArray)command.Result.Debug["Info"];

                    using (var session = store.OpenSession())
                    {
                        var postId = array[0].ToString();
                        var post = session.Load<Post>(postId);

                        Assert.Null(post);
                    }
                }
            }
        }
    }
}

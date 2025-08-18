using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi;

public class RavenDB_24648(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string HeartPngBase64 =
    "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAAGHaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8P3hwYWNrZXQgYmVnaW49J++7vycgaWQ9J1c1TTBNcENlaGlIenJlU3pOVGN6a2M5ZCc/Pg0KPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyI+PHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj48cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0idXVpZDpmYWY1YmRkNS1iYTNkLTExZGEtYWQzMS1kMzNkNzUxODJmMWIiIHhtbG5zOnRpZmY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vdGlmZi8xLjAvIj48dGlmZjpPcmllbnRhdGlvbj4xPC90aWZmOk9yaWVudGF0aW9uPjwvcmRmOkRlc2NyaXB0aW9uPjwvcmRmOlJERj48L3g6eG1wbWV0YT4NCjw/eHBhY2tldCBlbmQ9J3cnPz4slJgLAAAA1ElEQVRYR+2WORLDIAxF5Rwhvcvc/0Ap0+cKpMID4gtJmK3IqzwYfb2Rl+EIIQRayIMvzObQJvA9X9f18/PO7kl4akSBNATBg737I1BAC4vEUOt+AiKFgCesBS6QvYSjmxPosfwruAS42UjSXvtMYBV/gX0E+A9iJGmvfSawikxgxmPgPfaaAAHDnqDsQmA2UACZ3kXKhAJUKWihliUKkFJoRcuoCpAhoIalVhUgYxDHWmMSIEcgOfcWp2IL0vHN0zhinkAKaoTWLDRNoCdNE+jJcoEf1VNdHhBR9pYAAAAASUVORK5CYII=";

    private const string StarPngBase64 =
        "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";

    private const string NonEmptyAnswerHint =
        " ;Always provide a valid structured response matching the schema (if you have no answer or an empty answer - please return default values instead)";

    private class Post
    {
        public string Content { get; set; }
        public Comment[] Comments { get; set; }
        public Post(){ }
        public Post(string content, Comment[] comments)
        {
            Content = content;
            Comments = comments;
        }

        public override bool Equals(object obj)
        {
            if (obj is not Post other)
                return false;

            if (Content != other.Content)
                return false;

            if (Comments == other.Comments)
                return true;

            if (Comments == null || other.Comments == null || Comments.Length != other.Comments.Length)
                return false;

            for (int i = 0; i < Comments.Length; i++)
            {
                if (!Equals(Comments[i], other.Comments[i]))
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            int hash = Content?.GetHashCode() ?? 0;
            if (Comments != null)
            {
                foreach (var comment in Comments)
                    hash = HashCode.Combine(hash, comment?.GetHashCode() ?? 0);
            }

            return hash;
        }
    }

    private class Comment
    {
        public string Id { get; set; }
        public string Author { get; set; }
        public string Content { get; set; }
        public string AuthorDescription { get; set; }
        public string ProfileImage { get; set; }

        public Comment() { }

        public Comment(string id, string author, string content, string authorDescription, string profileImage)
        {
            Id = id;
            Author = author;
            Content = content;
            AuthorDescription = authorDescription;
            ProfileImage = profileImage;
        }


        public override bool Equals(object obj)
        {
            return obj is Comment other &&
                   Id == other.Id &&
                   Author == other.Author &&
                   Content == other.Content &&
                   AuthorDescription == other.AuthorDescription &&
                   ProfileImage == other.ProfileImage;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Id, Author, Content, AuthorDescription, ProfileImage);
        }


    }


    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task GenAiTestModeWithAttachments(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Describe the following images." + NonEmptyAnswerHint;
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(
            new { PhotoDescription = "Description of the photo" });

        config.UpdateScript = @"    
const comment = this.Comments.find(c => c.Id == $input.Id);
comment.AuthorDescription = $output.PhotoDescription;
";

        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    let img = loadAttachment(comment.ProfileImage);
    if(!img)
        continue;
    ai.genContext({Id: comment.Id}).withPng(img);
}"
        };

        var marker = "None" + Guid.NewGuid();
        var post1 = new Post("Hello World!",
            new Comment[]
            {
                new Comment(id: "Comment1", author: "Shahar Heart", authorDescription: marker, content: "Hey!", profileImage: "heart.png"),
                new Comment(id: "Comment2", author: "Omer Star", authorDescription: marker, content: "Hello!", profileImage: "star.png"),
                new Comment(id: "Comment3", author: "Aviv Rachmany", authorDescription: marker, content: "Hello", profileImage: "none.png")
            });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(post1, "Post/1");

            using var heart = new MemoryStream(Convert.FromBase64String(HeartPngBase64));
            using var star = new MemoryStream(Convert.FromBase64String(StarPngBase64));

            session.Advanced.Attachments.Store("Post/1", "heart.png", heart);
            session.Advanced.Attachments.Store("Post/1", "star.png", star);

            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        using var _ = database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        // Stage 1 : Create Gen Ai Contexts
        // test-doc (sending new doc - doesn't exist)
        var document = store.Conventions.Serialization.DefaultConverter.ToBlittable(post1, context);
        var createCtx2 = await store.Maintenance.SendAsync(context, new TestCreateGenAiContextOperation(document, config));
        Assert.Equal(0, createCtx2.Results.Count);


        // Existing doc with attachments
        var createCtx = await store.Maintenance.SendAsync(context, new TestCreateGenAiContextOperation("Post/1", config));
        var genAiContexts = createCtx.Results;
        Assert.Equal(2, genAiContexts.Count);
        Assert.NotNull(genAiContexts[0].ContextOutput.Context);
        Assert.NotNull(genAiContexts[1].ContextOutput.Context);
        Assert.Equal(1, genAiContexts[0].ContextOutput.Attachments.Count);
        Assert.Equal(1, genAiContexts[1].ContextOutput.Attachments.Count);
        Assert.Equal("heart.png", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("star.png", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("image/png", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Type);
        Assert.Equal("image/png", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Type);
        Assert.NotNull(genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.DataAsBase64);
        Assert.NotNull(genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.DataAsBase64);

        // Stage 2 : Send to model
        var sendToModel = await store.Maintenance.SendAsync(context, new TestGenAiSendToModelOperation(genAiContexts, config));
        var contextsAndOutputs = sendToModel.Results;
        Assert.Equal(2, contextsAndOutputs.Count);
        Assert.NotNull(contextsAndOutputs[0].ContextOutput.Context);
        Assert.NotNull(contextsAndOutputs[1].ContextOutput.Context);
        Assert.Equal(1, contextsAndOutputs[0].ContextOutput.Attachments.Count);
        Assert.Equal(1, contextsAndOutputs[1].ContextOutput.Attachments.Count);
        Assert.Equal("heart.png", contextsAndOutputs[0].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("star.png", contextsAndOutputs[1].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.NotNull(contextsAndOutputs[0].ModelOutput?.Output);
        Assert.NotNull(contextsAndOutputs[1].ModelOutput?.Output);
        
        // Stage 3 : Update Script
        // doc id
        var updateScriptRes = await store.Maintenance.SendAsync(context, new TestGenAiUpdateScriptOperation("Post/1", contextsAndOutputs, config));
        var inputDoc = ToPost(updateScriptRes.InputDocument);
        Assert.Equal(post1, inputDoc);
        var outputDoc = ToPost(updateScriptRes.OutputDocument);
        Assert.NotEqual(marker, outputDoc.Comments[0].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[1].AuthorDescription);
        outputDoc.Comments[0].AuthorDescription = marker;
        outputDoc.Comments[1].AuthorDescription = marker;
        Assert.Equal(post1, outputDoc);
        
        // doc
        updateScriptRes = await store.Maintenance.SendAsync(context, new TestGenAiUpdateScriptOperation(document, contextsAndOutputs, config));
        inputDoc = ToPost(updateScriptRes.InputDocument);
        Assert.Equal(post1, inputDoc);
        outputDoc = ToPost(updateScriptRes.OutputDocument);
        Assert.NotEqual(marker, outputDoc.Comments[0].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[1].AuthorDescription);
        outputDoc.Comments[0].AuthorDescription = marker;
        outputDoc.Comments[1].AuthorDescription = marker;
        Assert.Equal(post1, outputDoc);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task GenAiTestModeEditedDocWithAttachments(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Describe the following images." + NonEmptyAnswerHint;
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(
            new { PhotoDescription = "Description of the photo" });

        config.UpdateScript = @"    
const comment = this.Comments.find(c => c.Id == $input.Id);
comment.AuthorDescription = $output.PhotoDescription;
";

        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    let img = loadAttachment(comment.ProfileImage);
    if(!img)
        continue;
    ai.genContext({Id: comment.Id}).withPng(img);
}"
        };

        var marker = "None" + Guid.NewGuid();
        var post1 = new Post("Hello World!",
            new Comment[]
            {
                new Comment(id: "Comment1", author: "Shahar Heart", authorDescription: marker, content: "Hey!", profileImage: "heart.png"),
                new Comment(id: "Comment2", author: "Omer Star", authorDescription: marker, content: "Hello!", profileImage: "star.png"),
                new Comment(id: "Comment3", author: "Aviv Rachmany", authorDescription: marker, content: "Hello", profileImage: "none.png")
            });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(post1, "Post/1");

            using var heart = new MemoryStream(Convert.FromBase64String(HeartPngBase64));
            using var star = new MemoryStream(Convert.FromBase64String(StarPngBase64));

            session.Advanced.Attachments.Store("Post/1", "heart.png", heart);
            session.Advanced.Attachments.Store("Post/1", "star.png", star);

            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        using var _ = database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        // Stage 1 : Create Gen Ai Contexts
        // existing doc with edit
        var editedPost1 = new Post() { Content = "Shahar", Comments = post1.Comments };
        var document = store.Conventions.Serialization.DefaultConverter.ToBlittable(editedPost1, context);
        var createCtx3 = await store.Maintenance.SendAsync(context, new TestCreateGenAiContextOperation("Post/1", document, config));
        var genAiContexts = createCtx3.Results;
        Assert.Equal(2, genAiContexts.Count);
        Assert.NotNull(genAiContexts[0].ContextOutput.Context);
        Assert.NotNull(genAiContexts[1].ContextOutput.Context);
        Assert.Equal(1, genAiContexts[0].ContextOutput.Attachments.Count);
        Assert.Equal(1, genAiContexts[1].ContextOutput.Attachments.Count);
        Assert.Equal("heart.png", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("star.png", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("image/png", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Type);
        Assert.Equal("image/png", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Type);
        Assert.NotNull(genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.DataAsBase64);
        Assert.NotNull(genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.DataAsBase64);


        // Stage 2 : Send to model
        var sendToModel = await store.Maintenance.SendAsync(context, new TestGenAiSendToModelOperation(genAiContexts, config));
        var contextsAndOutputs = sendToModel.Results;
        Assert.Equal(2, contextsAndOutputs.Count);
        Assert.NotNull(contextsAndOutputs[0].ContextOutput.Context);
        Assert.NotNull(contextsAndOutputs[1].ContextOutput.Context);
        Assert.Equal(1, contextsAndOutputs[0].ContextOutput.Attachments.Count);
        Assert.Equal(1, contextsAndOutputs[1].ContextOutput.Attachments.Count);
        Assert.Equal("heart.png", contextsAndOutputs[0].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("star.png", contextsAndOutputs[1].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.NotNull(contextsAndOutputs[0].ModelOutput?.Output);
        Assert.NotNull(contextsAndOutputs[1].ModelOutput?.Output);

        // Stage 3 : Update Script
        // doc id
        var updateScriptRes = await store.Maintenance.SendAsync(context, new TestGenAiUpdateScriptOperation("Post/1", contextsAndOutputs, config));
        var inputDoc = ToPost(updateScriptRes.InputDocument);
        Assert.Equal(post1, inputDoc);
        var outputDoc = ToPost(updateScriptRes.OutputDocument);
        Assert.NotEqual(marker, outputDoc.Comments[0].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[1].AuthorDescription);
        outputDoc.Comments[0].AuthorDescription = marker;
        outputDoc.Comments[1].AuthorDescription = marker;
        Assert.Equal(post1, outputDoc);

        // doc
        updateScriptRes = await store.Maintenance.SendAsync(context, new TestGenAiUpdateScriptOperation(document, contextsAndOutputs, config));
        inputDoc = ToPost(updateScriptRes.InputDocument);
        Assert.Equal(editedPost1, inputDoc);
        outputDoc = ToPost(updateScriptRes.OutputDocument);
        Assert.NotEqual(marker, outputDoc.Comments[0].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[1].AuthorDescription);
        outputDoc.Comments[0].AuthorDescription = marker;
        outputDoc.Comments[1].AuthorDescription = marker;
        Assert.Equal(editedPost1, outputDoc);
    }

    private static readonly Func<BlittableJsonReaderObject, Post> ToPost = JsonDeserializationClient.GenerateJsonDeserializationRoutine<Post>();

    private class TestCreateGenAiContextOperation : IMaintenanceOperation<GenAiTestScriptResult>
    {
        private readonly TestGenAiScript _testGenAiScript;

        public TestCreateGenAiContextOperation(string docId, GenAiConfiguration config)
        {
            _testGenAiScript = new TestGenAiScript
            {
                DocumentId = docId,
                Configuration = config,
                TestStage = TestStage.CreateContextObjects
            };
        }

        public TestCreateGenAiContextOperation(BlittableJsonReaderObject doc, GenAiConfiguration config)
        {
            _testGenAiScript = new TestGenAiScript
            {
                Document = doc,
                Configuration = config,
                TestStage = TestStage.CreateContextObjects
            };
        }

        public TestCreateGenAiContextOperation(string docId, BlittableJsonReaderObject doc, GenAiConfiguration config)
        {
            _testGenAiScript = new TestGenAiScript
            {
                DocumentId = docId,
                Document = doc,
                Configuration = config,
                TestStage = TestStage.CreateContextObjects
            };
        }

        public RavenCommand<GenAiTestScriptResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new TestGenAiCommand(_testGenAiScript, conventions);
        }
    }

    private class TestGenAiSendToModelOperation : IMaintenanceOperation<GenAiTestScriptResult>
    {
        private readonly TestGenAiScript _testGenAiScript;

        public TestGenAiSendToModelOperation(List<GenAiResultItem> genAiContexts, GenAiConfiguration config)
        {
            _testGenAiScript = new TestGenAiScript
            {
                Input = genAiContexts,
                Configuration = config,
                TestStage = TestStage.SendToModel
            };
        }

        public RavenCommand<GenAiTestScriptResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new TestGenAiCommand(_testGenAiScript, conventions);
        }
    }

    private class TestGenAiUpdateScriptOperation : IMaintenanceOperation<GenAiTestScriptResult>
    {
        private readonly TestGenAiScript _testGenAiScript;

        public TestGenAiUpdateScriptOperation(string docId, List<GenAiResultItem> genAiContextsAndModelOutputs, GenAiConfiguration config)
        {
            _testGenAiScript = new TestGenAiScript
            {
                DocumentId = docId,
                Input = genAiContextsAndModelOutputs,
                Configuration = config,
                TestStage = TestStage.ApplyUpdateScript
            };
        }

        public TestGenAiUpdateScriptOperation(BlittableJsonReaderObject doc, List<GenAiResultItem> genAiContextsAndModelOutputs, GenAiConfiguration config)
        {
            _testGenAiScript = new TestGenAiScript
            {
                Document = doc,
                Input = genAiContextsAndModelOutputs,
                Configuration = config,
                TestStage = TestStage.ApplyUpdateScript
            };
        }

        public RavenCommand<GenAiTestScriptResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new TestGenAiCommand(_testGenAiScript, conventions);
        }
    }

    private class TestGenAiCommand(TestGenAiScript testGenAiScript, DocumentConventions conventions) : RavenCommand<GenAiTestScriptResult>
    {
        private readonly TestGenAiScript _testGenAiScript = testGenAiScript;
        private readonly DocumentConventions _conventions = conventions;

        public override bool IsReadRequest { get; } = true;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/ai/gen-ai/test";
            var bjro = ctx.ReadObject(_testGenAiScript.ToJson(), "TestGenAiCommand_payload");
            return new HttpRequestMessage
            {
                Method = HttpMethods.Post,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, bjro).ConfigureAwait(false), _conventions)
            };
        }
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = DeserializeToTestEtlScriptResult(response);
        }

        private static readonly Func<BlittableJsonReaderObject, GenAiTestScriptResult> DeserializeToTestEtlScriptResult = JsonDeserializationClient.GenerateJsonDeserializationRoutine<GenAiTestScriptResult>();

    }
}

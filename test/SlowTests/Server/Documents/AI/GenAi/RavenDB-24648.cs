using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
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
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi;

public class RavenDB_24648(ITestOutputHelper output) : RavenTestBase(output)
{
    private static readonly string BananaPngBase64 = GetBananaBase64();

    private const string NonEmptyAnswerHint =
        " ;Always provide a valid structured response matching the schema (if you have no answer or an empty answer - please return default values instead)";

    private static ReadOnlySpan<byte> Text => "emoji_אימוג'י_\ud83d\ude00\ud83d\ude01\ud83d\ude02\ud83d\ude0d\ud83d\ude0e\ud83d\ude4f\ud83e\udd14\ud83d\ude80\ud83d\udc31\ud83d\ude00\ud83d\ude01\ud83d\ude02\ud83d\ude0d\ud83d\ude0e\ud83d\ude4f\ud83e\udd14\ud83d\ude80_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji_emoji"u8;

    private static ReadOnlySpan<byte> Csv => 
        @"Date,Description,Category,Amount
2025-01-01,Grocery Store,food,45.32
2025-01-02,Utility Bill,Utilities,120.75
2025-01-03,Online Shopping,Retail,89.99
2025-01-04,Gas Station,Transportation,35.50
2025-01-05,Restaurant,Food,62.10
2025-01-06,Internet Bill,Utilities,79.99
2025-01-07,Pharmacy,Health,22.45
2025-01-08,Streaming Service,Entertainment,14.99
2025-01-09,Gym Membership,Fitness,40.00
2025-01-10,Clothing Store,Retail,75.20
2025-01-11,Coffee Shop,Food,8.75
2025-01-12,Car Insurance,Transportation,95.00
2025-01-13,Home Depot,Home,130.25
2025-01-14,Pet Supplies,Pet Care,28.60
2025-01-15,Doctor Visit,Health,50.00
2025-01-16,Grocery Store,Food,53.80
2025-01-17,Movie Theater,Entertainment,22.00
2025-01-18,Phone Bill,Utilities,65.30
2025-01-19,Bookstore,Retail,19.95
2025-01-20,Auto Repair,Transportation,210.50
2025-01-21,Fast Food,Food,12.65
2025-01-22,Charity Donation,Miscellaneous,25.00
2025-01-23,Hair Salon,Personal Care,45.00
2025-01-24,Grocery Store,Food,60.15
2025-01-25,Online Subscription,Entertainment,9.99"u8;

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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
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

            await using var heart = GetFileAsStream("heart.png");
            await using var star = GetFileAsStream("star.png");

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
        Assert.NotNull(genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.NotNull(genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Data);

        // Stage 2 : Send to model
        var sendToModel = await store.Maintenance.SendAsync(context, new TestGenAiSendToModelOperation(genAiContexts, config));
        var contextsAndOutputs = sendToModel.Results;
        Assert.Equal(2, contextsAndOutputs.Count);
        Assert.NotNull(contextsAndOutputs[0].ContextOutput.Context);
        Assert.NotNull(contextsAndOutputs[1].ContextOutput.Context);
        Assert.Equal(0, contextsAndOutputs[0].ContextOutput.Attachments.Count);
        Assert.Equal(0, contextsAndOutputs[1].ContextOutput.Attachments.Count);
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
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

            await using var heart = GetFileAsStream("heart.png");
            await using var star = GetFileAsStream("star.png");

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
        Assert.NotNull(genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.NotNull(genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Data);


        // Stage 2 : Send to model
        var sendToModel = await store.Maintenance.SendAsync(context, new TestGenAiSendToModelOperation(genAiContexts, config));
        var contextsAndOutputs = sendToModel.Results;
        Assert.Equal(2, contextsAndOutputs.Count);
        Assert.NotNull(contextsAndOutputs[0].ContextOutput.Context);
        Assert.Equal(0, contextsAndOutputs[0].ContextOutput.Attachments.Count);
        Assert.NotNull(contextsAndOutputs[1].ContextOutput.Context);
        Assert.Equal(0, contextsAndOutputs[1].ContextOutput.Attachments.Count);
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

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiTestModeWithNotFoundAttachments(Options options, GenAiConfiguration config)
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

            await using var heart = GetFileAsStream("heart.png");
            await using var star = GetFileAsStream("star.png");

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
        var genAiContexts2 = createCtx2.Results;
        Assert.Equal(3, genAiContexts2.Count);
        var attNames = new string[] { "heart.png", "star.png", "none.png" };
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(1, genAiContexts2[i].ContextOutput.Attachments.Count);
            Assert.Equal(attNames[i], genAiContexts2[i].ContextOutput.Attachments.FirstOrDefault()?.Name);
            Assert.Equal(string.Empty, genAiContexts2[i].ContextOutput.Attachments.FirstOrDefault()?.Data);
            Assert.Equal(AiAttachmentSource.NotFound, genAiContexts2[i].ContextOutput.Attachments.FirstOrDefault()?.Source);
            Assert.Equal("image/png", genAiContexts2[0].ContextOutput.Attachments.FirstOrDefault()?.Type);
        }

        // Existing doc with attachments
        var createCtx = await store.Maintenance.SendAsync(context, new TestCreateGenAiContextOperation("Post/1", config));
        var genAiContexts = createCtx.Results;
        Assert.Equal(3, genAiContexts.Count);
        Assert.NotNull(genAiContexts[0].ContextOutput.Context);
        Assert.NotNull(genAiContexts[1].ContextOutput.Context);
        Assert.Equal(1, genAiContexts[0].ContextOutput.Attachments.Count);
        Assert.Equal(1, genAiContexts[1].ContextOutput.Attachments.Count);
        Assert.Equal("heart.png", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("star.png", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("image/png", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Type);
        Assert.Equal("image/png", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Type);
        Assert.Equal("[Hash:'FLNK25A3VOpVPIiusBEZMwUU5mWqSZR7T2OqYF4nBfA=']", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.Equal("[Hash:'94IlaZrchKnAQBD3vQQ7sE0Yt6f0CJeu69Ljfb66bxo=']", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.Equal(AiAttachmentSource.FromAttachment, genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Source);
        Assert.Equal(AiAttachmentSource.FromAttachment, genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Source);
        Assert.NotNull(genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.NotNull(genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Data);

        Assert.Equal(1, genAiContexts[2].ContextOutput.Attachments.Count);
        Assert.Equal("none.png", genAiContexts[2].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal(string.Empty, genAiContexts[2].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.Equal(AiAttachmentSource.NotFound, genAiContexts[2].ContextOutput.Attachments.FirstOrDefault()?.Source);
        Assert.Equal("image/png", genAiContexts[2].ContextOutput.Attachments.FirstOrDefault()?.Type);


        // Stage 2 : Send to model
        var sendToModel = await store.Maintenance.SendAsync(context, new TestGenAiSendToModelOperation(genAiContexts, config));
        var contextsAndOutputs = sendToModel.Results;
        Assert.Equal(3, contextsAndOutputs.Count);
        Assert.NotNull(contextsAndOutputs[0].ContextOutput.Context);
        Assert.NotNull(contextsAndOutputs[1].ContextOutput.Context);
        Assert.Equal(0, contextsAndOutputs[0].ContextOutput.Attachments.Count);
        Assert.Equal(0, contextsAndOutputs[1].ContextOutput.Attachments.Count);
        Assert.NotNull(contextsAndOutputs[0].ModelOutput?.Output);
        Assert.NotNull(contextsAndOutputs[1].ModelOutput?.Output);
        Assert.NotNull(contextsAndOutputs[2].ModelOutput?.Output);
        
        // Stage 3 : Update Script
        // doc id
        var updateScriptRes = await store.Maintenance.SendAsync(context, new TestGenAiUpdateScriptOperation("Post/1", contextsAndOutputs, config));
        var inputDoc = ToPost(updateScriptRes.InputDocument);
        Assert.Equal(post1, inputDoc);
        var outputDoc = ToPost(updateScriptRes.OutputDocument);
        Assert.NotEqual(marker, outputDoc.Comments[0].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[1].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[2].AuthorDescription);

        outputDoc.Comments[0].AuthorDescription = marker;
        outputDoc.Comments[1].AuthorDescription = marker;
        outputDoc.Comments[2].AuthorDescription = marker;
        Assert.Equal(post1, outputDoc);
        
        // doc
        updateScriptRes = await store.Maintenance.SendAsync(context, new TestGenAiUpdateScriptOperation(document, contextsAndOutputs, config));
        inputDoc = ToPost(updateScriptRes.InputDocument);
        Assert.Equal(post1, inputDoc);
        outputDoc = ToPost(updateScriptRes.OutputDocument);
        Assert.NotEqual(marker, outputDoc.Comments[0].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[1].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[2].AuthorDescription);
        outputDoc.Comments[0].AuthorDescription = marker;
        outputDoc.Comments[1].AuthorDescription = marker;
        outputDoc.Comments[2].AuthorDescription = marker;
        Assert.Equal(post1, outputDoc);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiTestModeWithUnloadedAttachments(Options options, GenAiConfiguration config)
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
            Script = $"const banana = '{BananaPngBase64}'; " +
                @"
for(const comment of this.Comments)
{
    let img = loadAttachment(comment.ProfileImage);
    if (!img){
        ai.genContext({Id: comment.Id}).withPng(banana);
        continue;
    }
        
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

            await using var heart = GetFileAsStream("heart.png");
            await using var star = GetFileAsStream("star.png");

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
        var genAiContexts2 = createCtx2.Results;
        Assert.Equal(3, genAiContexts2.Count);
        for (int i = 0; i < 2; i++)
        {
            Assert.Equal(1, genAiContexts2[i].ContextOutput.Attachments.Count);
            var att = genAiContexts2[0].ContextOutput.Attachments.First();
            Assert.Equal("unknown.name", att.Name);
            Assert.Equal(AiAttachmentSource.FromScript, att.Source);
            Assert.Equal("image/png", att.Type);
            Assert.Equal(BananaPngBase64, att.Data);
        }

        // Existing doc with attachments
        var createCtx = await store.Maintenance.SendAsync(context, new TestCreateGenAiContextOperation("Post/1", config));
        var genAiContexts = createCtx.Results;
        Assert.Equal(3, genAiContexts.Count);
        Assert.NotNull(genAiContexts[0].ContextOutput.Context);
        Assert.NotNull(genAiContexts[1].ContextOutput.Context);
        Assert.Equal(1, genAiContexts[0].ContextOutput.Attachments.Count);
        Assert.Equal(1, genAiContexts[1].ContextOutput.Attachments.Count);
        Assert.Equal("heart.png", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("star.png", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal("image/png", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Type);
        Assert.Equal("image/png", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Type);
        Assert.Equal("[Hash:'FLNK25A3VOpVPIiusBEZMwUU5mWqSZR7T2OqYF4nBfA=']", genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.Equal("[Hash:'94IlaZrchKnAQBD3vQQ7sE0Yt6f0CJeu69Ljfb66bxo=']", genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.Equal(AiAttachmentSource.FromAttachment, genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Source);
        Assert.Equal(AiAttachmentSource.FromAttachment, genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Source);
        Assert.NotNull(genAiContexts[0].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.NotNull(genAiContexts[1].ContextOutput.Attachments.FirstOrDefault()?.Data);
        
        Assert.Equal(1, genAiContexts[2].ContextOutput.Attachments.Count);
        Assert.Equal("unknown.name", genAiContexts[2].ContextOutput.Attachments.FirstOrDefault()?.Name);
        Assert.Equal(BananaPngBase64, genAiContexts[2].ContextOutput.Attachments.FirstOrDefault()?.Data);
        Assert.Equal(AiAttachmentSource.FromScript, genAiContexts[2].ContextOutput.Attachments.FirstOrDefault()?.Source);
        Assert.Equal("image/png", genAiContexts[2].ContextOutput.Attachments.FirstOrDefault()?.Type);
        
        
        // Stage 2 : Send to model
        var sendToModel = await store.Maintenance.SendAsync(context, new TestGenAiSendToModelOperation(genAiContexts, config));
        var contextsAndOutputs = sendToModel.Results;
        Assert.Equal(3, contextsAndOutputs.Count);
        Assert.NotNull(contextsAndOutputs[0].ContextOutput.Context);
        Assert.NotNull(contextsAndOutputs[1].ContextOutput.Context);
        Assert.Equal(0, contextsAndOutputs[0].ContextOutput.Attachments.Count);
        Assert.Equal(0, contextsAndOutputs[1].ContextOutput.Attachments.Count);
        Assert.NotNull(contextsAndOutputs[0].ModelOutput?.Output);
        Assert.NotNull(contextsAndOutputs[1].ModelOutput?.Output);
        Assert.NotNull(contextsAndOutputs[2].ModelOutput?.Output);
        
        // Stage 3 : Update Script
        // doc id
        var updateScriptRes = await store.Maintenance.SendAsync(context, new TestGenAiUpdateScriptOperation("Post/1", contextsAndOutputs, config));
        var inputDoc = ToPost(updateScriptRes.InputDocument);
        Assert.Equal(post1, inputDoc);
        var outputDoc = ToPost(updateScriptRes.OutputDocument);
        Assert.NotEqual(marker, outputDoc.Comments[0].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[1].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[2].AuthorDescription);
        
        outputDoc.Comments[0].AuthorDescription = marker;
        outputDoc.Comments[1].AuthorDescription = marker;
        outputDoc.Comments[2].AuthorDescription = marker;
        Assert.Equal(post1, outputDoc);
        
        // doc
        updateScriptRes = await store.Maintenance.SendAsync(context, new TestGenAiUpdateScriptOperation(document, contextsAndOutputs, config));
        inputDoc = ToPost(updateScriptRes.InputDocument);
        Assert.Equal(post1, inputDoc);
        outputDoc = ToPost(updateScriptRes.OutputDocument);
        Assert.NotEqual(marker, outputDoc.Comments[0].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[1].AuthorDescription);
        Assert.NotEqual(marker, outputDoc.Comments[2].AuthorDescription);
        outputDoc.Comments[0].AuthorDescription = marker;
        outputDoc.Comments[1].AuthorDescription = marker;
        outputDoc.Comments[2].AuthorDescription = marker;
        Assert.Equal(post1, outputDoc);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ContextsWithAllKindsOfDocs(Options options, GenAiConfiguration config)
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
            Script = $"const banana = '{BananaPngBase64}'; " +
@"
for(const comment of this.Comments)
{
    if (comment.ProfileImage === 'all'){
        ai.genContext({Id: comment.Id})
            .withPng(loadAttachment('heart.png'))
            .withPng(loadAttachment('star.png'))
            .withPng(loadAttachment('none.png'))
            .withPng(banana)
            .withText(loadAttachment('transactions.csv'))
            .withText(loadAttachment('text.csv'))
            .withText(loadAttachment('short_text.txt'));
    }
}"
        };

        var marker = "None" + Guid.NewGuid();
        var post1 = new Post("Hello World!",
            new Comment[]
            {
                new Comment(id: "Comment0", author: "Shahar All", authorDescription: marker, content: "Hi!", profileImage: "all"),
                new Comment(id: "Comment1", author: "Aviv All", authorDescription: marker, content: "Hi!", profileImage: "all"),
            });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(post1, "Post/1");

            await using var heart = GetFileAsStream("heart.png");
            await using var star = GetFileAsStream("star.png");
            using var csv = new MemoryStream(Csv.ToArray());
            using var text = new MemoryStream(Text.ToArray());
            using var shortText = new MemoryStream("sort text"u8.ToArray());

            session.Advanced.Attachments.Store("Post/1", "heart.png", heart);
            session.Advanced.Attachments.Store("Post/1", "star.png", star);
            session.Advanced.Attachments.Store("Post/1", "transactions.csv", csv);
            session.Advanced.Attachments.Store("Post/1", "text.csv", text);
            session.Advanced.Attachments.Store("Post/1", "short_text.txt", shortText);

            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        using var _ = database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        // Existing doc with attachments
        var createCtx = await store.Maintenance.SendAsync(context, new TestCreateGenAiContextOperation("Post/1", config));
        var genAiContexts = createCtx.Results;
        Assert.Equal(2, genAiContexts.Count);
        foreach (var genAiContext in genAiContexts)
        {
            var aiAttachments = genAiContext.ContextOutput.Attachments;
            Assert.Equal(7, aiAttachments.Count);
            Assert.Equal("heart.png", aiAttachments[0].Name);
            Assert.Equal("star.png", aiAttachments[1].Name);
            Assert.Equal("none.png", aiAttachments[2].Name);
            Assert.Equal("unknown.name", aiAttachments[3].Name);
            Assert.Equal("transactions.csv", aiAttachments[4].Name);
            Assert.Equal("text.csv", aiAttachments[5].Name);
            Assert.Equal("short_text.txt", aiAttachments[6].Name);

            Assert.Equal(AiAttachmentSource.FromAttachment, aiAttachments[0].Source);
            Assert.Equal(AiAttachmentSource.FromAttachment, aiAttachments[1].Source);
            Assert.Equal(AiAttachmentSource.NotFound, aiAttachments[2].Source);
            Assert.Equal(AiAttachmentSource.FromScript, aiAttachments[3].Source);
            Assert.Equal("[Hash:'FLNK25A3VOpVPIiusBEZMwUU5mWqSZR7T2OqYF4nBfA=']", aiAttachments[0].Data);
            Assert.Equal("[Hash:'94IlaZrchKnAQBD3vQQ7sE0Yt6f0CJeu69Ljfb66bxo=']", aiAttachments[1].Data);
            Assert.Equal(string.Empty, aiAttachments[2].Data);
            Assert.Equal(BananaPngBase64, aiAttachments[3].Data);
            Assert.Equal(Encoding.UTF8.GetString(Csv).Substring(0, 100) + "...", aiAttachments[4].Data);
            Assert.Equal("emoji_אימוג'י_\ud83d\ude00\ud83d\ude01\ud83d\ude02\ud83d\ude0d\ud83d\ude0e\ud83d\ude4f\ud83e\udd14\ud83d\ude80\ud83d\udc31\ud83d\ude00\ud83d\ude01\ud83d\ude02\ud83d\ude0d\ud83d\ude0e\ud83d\ude4f\ud83e\udd14\ud83d\ude80_emoji_emoji...", aiAttachments[5].Data);
            Assert.Equal("sort text", aiAttachments[6].Data);
        }
    }

    private static string GetBananaBase64()
    {
        using var file = GetFileAsStream("banana.png");

        using var memoryStream = new MemoryStream();

        using var transform = new ToBase64Transform();
        using var cryptoStream = new CryptoStream(file, transform, CryptoStreamMode.Read);
        cryptoStream.CopyTo(memoryStream);

        Span<byte> readOnlySpan = memoryStream.GetBuffer();
        return Encoding.UTF8.GetString(readOnlySpan[..(int)memoryStream.Length]);
    }

    private static Stream GetFileAsStream(string fileName)
    {
        var assembly = typeof(RavenDB_24648).Assembly;
        var resourceName = "SlowTests.Data.RavenDB_24648." + fileName;

        return assembly.GetManifestResourceStream(resourceName)
               ?? throw new FileNotFoundException($"Embedded resource not found: {resourceName}");
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
        public override bool IsReadRequest { get; } = true;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/ai/gen-ai/test";
            var bjro = ctx.ReadObject(testGenAiScript.ToJson(), "TestGenAiCommand_payload");
            return new HttpRequestMessage
            {
                Method = HttpMethods.Post,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, bjro).ConfigureAwait(false), conventions)
            };
        }
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = DeserializeToTestEtlScriptResult(response);
        }

        private static readonly Func<BlittableJsonReaderObject, GenAiTestScriptResult> DeserializeToTestEtlScriptResult = JsonDeserializationClient.GenerateJsonDeserializationRoutine<GenAiTestScriptResult>();

    }
}

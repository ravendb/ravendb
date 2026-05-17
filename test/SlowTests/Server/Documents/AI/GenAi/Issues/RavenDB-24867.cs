using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.ETL;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_24867(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string NonEmptyAnswerHint =
        " ;Always provide a valid structured response matching the schema (if you have no answer or an empty answer - please return default values instead)";

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task PngAsBase64Directly(Options options, GenAiConfiguration config)
    {
        string script = 
            $"const heart = '{Data.HeartPngBase64}'; const star = '{Data.StarPngBase64}'; " +
            @"
if (this.Name === 'shahar') {
    ai.genContext({}).withPng(heart);
    return;
}

if (this.Name === 'aviv') {
    ai.genContext({}).withPng(star);
    return;
}
";

        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Describe the following images." + NonEmptyAnswerHint;
        config.Collection = "Users";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            ImageDescriptions = new[]
            {
                new { Description = "Detailed description of the image", SafeForWork = true, Tags = new[] { "matching tags for the image" } }
            }
        });
        config.UpdateScript = @"this.ImageDescriptions = $output.ImageDescriptions;";
        config.GenAiTransformation = new GenAiTransformation { Script = script };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var etl = Etl.WaitForEtlToComplete(store);

        var marker = new[] { new ImageDescription("None" + Guid.NewGuid(), false, null) };

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User()
            {
                Id = "Users/1",
                Name = "shahar",
                ImageDescriptions = marker
            });
            await session.StoreAsync(new User()
            {
                Id = "Users/2",
                Name = "aviv",
                ImageDescriptions = marker
            });            
            await session.StoreAsync(new User()
            {
                Id = "Users/3",
                Name = "karmel",
                ImageDescriptions = marker
            });

            await session.SaveChangesAsync();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        using (var session = store.OpenAsyncSession())
        {
            var user1 = await session.LoadAsync<User>("Users/1");
            var user2 = await session.LoadAsync<User>("Users/2");
            var user3 = await session.LoadAsync<User>("Users/3");

            Assert.True(user1.ImageDescriptions == null || user1.ImageDescriptions.Length == 0 || marker[0].Description != user1.ImageDescriptions[0].Description); // changed
            Assert.True(user2.ImageDescriptions == null || user2.ImageDescriptions.Length == 0 || marker[0].Description != user2.ImageDescriptions[0].Description); // changed
            Assert.Equal(marker[0].Description, user3.ImageDescriptions[0].Description);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task PngNotAsBase64ShouldThrow(Options options, GenAiConfiguration config)
    {
        string script =
            $"const heart = '{Data.HeartPngBase64}'; " +
            @"
if (this.Name === 'Shahar') {
    ai.genContext({}).withPng(heart);
    return;
}

if (this.Name === 'Aviv') {
    ai.genContext({}).withPng('This is a star');
    return;
}
";

        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Describe the following images." + NonEmptyAnswerHint;
        config.Collection = "Users";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            ImageDescriptions = new[]
            {
                new { Description = "Detailed description of the image", SafeForWork = true, Tags = new[] { "matching tags for the image" } }
            }
        });
        config.UpdateScript = @"this.ImageDescriptions = $output.ImageDescriptions;";
        config.GenAiTransformation = new GenAiTransformation { Script = script };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var etl = Etl.WaitForEtlToComplete(store);

        var marker = new[] { new ImageDescription("None" + Guid.NewGuid(), false, null) };

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User()
            {
                Id = "Users/1",
                Name = "Shahar",
                ImageDescriptions = marker
            });
            await session.StoreAsync(new User()
            {
                Id = "Users/2",
                Name = "Aviv",
                ImageDescriptions = marker
            });
            await session.StoreAsync(new User()
            {
                Id = "Users/3",
                Name = "Karmel",
                ImageDescriptions = marker
            });

            await session.SaveChangesAsync();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        using (var session = store.OpenAsyncSession())
        {
            var user1 = await session.LoadAsync<User>("Users/1");
            var user2 = await session.LoadAsync<User>("Users/2");
            var user3 = await session.LoadAsync<User>("Users/3");
        
            Assert.True(user1.ImageDescriptions == null || user1.ImageDescriptions.Length == 0 || marker[0].Description != user1.ImageDescriptions[0].Description); // changed
            Assert.Equal(marker[0].Description, user2.ImageDescriptions[0].Description);
            Assert.Equal(marker[0].Description, user3.ImageDescriptions[0].Description);
        }

        IEnumerable<TaskItemErrorTableValue> errors = null;
        var hasError = await WaitForValueAsync(async () =>
        {
            errors = await Etl.GetItemTransformationErrorsAsync(store.Database, config);
            return errors.Any(e => e.DocumentId == "Users/2" && e.Error.Contains("Attachment must be loaded or base64 string (on type image/png)"));
        }, true, timeout: 60_000);

        Assert.True(hasError, $"Expected transformation error for Users/2 not found. Errors: {string.Join(", ", errors?.Select(e => $"{e.DocumentId}: {e.Error}") ?? Array.Empty<string>())}");
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public ImageDescription[] ImageDescriptions { get; set; }
    }
    public record ImageDescription(string Description, bool SafeForWork, string[] Tags);
}

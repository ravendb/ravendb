using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Tests.Infrastructure;
using Xunit;
using Task = System.Threading.Tasks.Task;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_24645(ITestOutputHelper output) : RavenTestBase(output)
    {
        public record Item(ImageDescription ImageDescription = null);

        public record ImageDescription(string Description, bool SafeForWork, string[] Tags);

        private const string NonEmptyAnswerHint =
            " ;Always provide a valid structured response matching the schema do not return an empty answer";

        private const string FormatScript = @"
ai.genContext({})
    .withExpected(loadAttachment('image.Actual'));
";

        private const string Image = "image.";

        private const string FirstItemId = "items/1";

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Png", "jpeg" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Png", "webp" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Png", "gif" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Png", "png" })]

        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Jpeg", "png" })]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Jpeg", "webp" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Jpeg", "gif" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Jpeg", "jpeg" })]

        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Webp", "png" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Webp", "jpeg" })]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Webp", "gif" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Webp", "webp" })]

        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Gif", "png" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Gif", "jpeg" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Gif", "webp" })]
        // [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "Gif", "gif" })]

        public async Task CanUseImageWithTheWrongFormat(Options options, GenAiConfiguration config, string expectedFormat, string actualFormat)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            config.Prompt = "Describe the following images" + NonEmptyAnswerHint;
            config.Collection = "Items";
            config.SampleObject = JsonConvert.SerializeObject(new
            {
                ImageDescription = new
                {
                    Description = "Detailed description of the image",
                    SafeForWork = true,
                    Tags = new[]
                    {
                        "matching tags for the image"
                    }
                }
            });
            config.UpdateScript = @"this.ImageDescription = $output.ImageDescription;";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = FormatScript
                    .Replace("Expected" , expectedFormat)
                    .Replace("Actual", actualFormat)
            };
            await store.Maintenance.SendAsync(new AddGenAiOperation(config));
        
            var etl = Etl.WaitForEtlToComplete(store);
            var marker = new ImageDescription("None" + Guid.NewGuid(), false, new string[] { "None" });
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(marker), FirstItemId);
                await using var file1 = GetEmbeddedImgStream(actualFormat);
        
                session.Advanced.Attachments.Store(FirstItemId, Image+actualFormat, file1);
                await session.SaveChangesAsync();
            }
            Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 30)));

            var transformationErrors = (await Etl.GetItemTransformationErrorsAsync(store.Database, config)).ToList();
            var loadErrors = (await Etl.GetItemLoadErrorsAsync(store.Database, config)).ToList();
            Assert.Empty(transformationErrors);
            Assert.Empty(loadErrors);

            using (var session = store.OpenAsyncSession())
            {
                var item1 = await session.LoadAsync<Item>(FirstItemId);
                Assert.NotNull(item1.ImageDescription);
                Assert.Contains("heart", item1.ImageDescription.Description.ToLower());
                Assert.False(item1.ImageDescription.Description == marker.Description);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "png" })]
        public async Task CanUseImageActualName(Options options, GenAiConfiguration config, string format)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = "Describe the following images" + NonEmptyAnswerHint;
            config.Collection = "Items";
            config.SampleObject = JsonConvert.SerializeObject(new
            {
                ImageDescriptions = new[]
                {
                    new { Description = "Detailed description of the image", SafeForWork = true, Tags = new[] { "matching tags for the image" } }
                }
            });
            config.UpdateScript = @"this.ImageDescriptions = $output.ImageDescriptions;";
            config.GenAiTransformation = new GenAiTransformation { Script = @"
ai.genContext({})
    .withPng('image.png');
" };

            await store.Maintenance.SendAsync(new AddGenAiOperation(config));
            var etl = Etl.WaitForEtlToComplete(store);
            var marker = new ImageDescription("None" + Guid.NewGuid(), false, new string[] { "None" });
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(marker), FirstItemId);
                await using var file1 = GetEmbeddedImgStream(format);

                session.Advanced.Attachments.Store(FirstItemId, "image.png", file1);
                await session.SaveChangesAsync();
            }
            await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 30));

            IEnumerable<TaskItemErrorTableValue> errors = null;
            var hasError = await WaitForValueAsync(async () =>
            {
                errors = await Etl.GetItemTransformationErrorsAsync(store.Database, config);
                return errors.Any(e => e.Error.Contains("Attachment must be loaded or base64 string (on type image/png)"));
            }, true, timeout: 60_000);

            Assert.True(hasError, $"Expected transformation error not found. Errors: {string.Join(", ", errors?.Select(e => $"{e.DocumentId}: {e.Error}") ?? Array.Empty<string>())}");
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "png" })]
        public async Task ShouldNotifyOnInvalidImage(Options options, GenAiConfiguration config, string format)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = "Describe the following images" + NonEmptyAnswerHint;
            config.Collection = "Items";
            config.SampleObject = JsonConvert.SerializeObject(new
            {
                ImageDescriptions = new[]
                {
                    new { Description = "Detailed description of the image", SafeForWork = true, Tags = new[] { "matching tags for the image" } }
                }
            });
            config.UpdateScript = @"this.ImageDescriptions = $output.ImageDescriptions;";

            config.GenAiTransformation = new GenAiTransformation { Script = @"
const att = loadAttachment('image.png');

ai.genContext({})
    .withPng(att);
" };

            await store.Maintenance.SendAsync(new AddGenAiOperation(config));

            var etl = Etl.WaitForEtlToComplete(store);

            var marker = new ImageDescription("None" + Guid.NewGuid(), false, new string[] { "None" });
            var markerArr = new ImageDescription[] { marker };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(marker), FirstItemId);
                var notValidBase64Att = GetEmbeddedImgBytes(format);
                notValidBase64Att[0] = 0xFF;
                using var file1 = new MemoryStream(notValidBase64Att);

                session.Advanced.Attachments.Store(FirstItemId, "image.png", file1);
                await session.SaveChangesAsync();
            }
            Assert.False(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 30)));

            var expectedError = config.Connection.OpenAiSettings != null
                ? "You uploaded an unsupported image."
                : "Unable to process input image";

            IEnumerable<TaskItemErrorTableValue> errors = null;
            var hasError = await WaitForValueAsync(async () =>
            {
                errors = await Etl.GetItemLoadErrorsAsync(store.Database, config);
                return errors.Any(e => e.Error.Contains(expectedError));
            }, true, timeout: 60_000);

            Assert.True(hasError, $"Expected error containing '{expectedError}' not found. Errors: {string.Join(", ", errors?.Select(e => $"{e.DocumentId}: {e.Error}") ?? Array.Empty<string>())}");
        }

        private static Stream GetEmbeddedImgStream(string format)
        {
            var asm = typeof(RavenDB_24645).Assembly;
            var resourceName = "SlowTests.Data.RavenDB_24645.heart." + format;

            var stream = asm.GetManifestResourceStream(resourceName);
            if (stream == null)
                throw new FileNotFoundException($"Embedded resource not found: {resourceName}\n" +
                                                "Check Build Action = Embedded Resource and the path/casing.");

            return stream;
        }

        public static string PrintNotificationErrors(DocumentDatabase db)
        {
            using (db.NotificationCenter.GetStored(out var actions))
            {
                var jsonAlerts = actions.ToList();
                if (jsonAlerts.Count == 0)
                    return null;

                var fullNotificationsJson = string.Join(
                    Environment.NewLine,
                    jsonAlerts.Select(a => a.Json?.ToString() ?? string.Empty));

                return fullNotificationsJson;
            }
        }

        private static byte[] GetEmbeddedImgBytes(string format)
        {
            using var s = GetEmbeddedImgStream(format);
            using var ms = new MemoryStream();
            s.CopyTo(ms);
            return ms.ToArray();
        }
    }
}

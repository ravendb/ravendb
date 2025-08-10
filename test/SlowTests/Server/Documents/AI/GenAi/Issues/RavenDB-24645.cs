using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using FastTests;
using Microsoft.Azure.Documents.SystemFunctions;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Task = System.Threading.Tasks.Task;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_24645(ITestOutputHelper output) : RavenTestBase(output)
    {
        public record Item(ImageDescription[] ImageDescriptions = null);

        public record ImageDescription(string Description, bool SafeForWork, string[] Tags);

        private const string HeartPngBase64 =
            "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAAGHaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8P3hwYWNrZXQgYmVnaW49J++7vycgaWQ9J1c1TTBNcENlaGlIenJlU3pOVGN6a2M5ZCc/Pg0KPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyI+PHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj48cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0idXVpZDpmYWY1YmRkNS1iYTNkLTExZGEtYWQzMS1kMzNkNzUxODJmMWIiIHhtbG5zOnRpZmY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vdGlmZi8xLjAvIj48dGlmZjpPcmllbnRhdGlvbj4xPC90aWZmOk9yaWVudGF0aW9uPjwvcmRmOkRlc2NyaXB0aW9uPjwvcmRmOlJERj48L3g6eG1wbWV0YT4NCjw/eHBhY2tldCBlbmQ9J3cnPz4slJgLAAAA1ElEQVRYR+2WORLDIAxF5Rwhvcvc/0Ap0+cKpMID4gtJmK3IqzwYfb2Rl+EIIQRayIMvzObQJvA9X9f18/PO7kl4akSBNATBg737I1BAC4vEUOt+AiKFgCesBS6QvYSjmxPosfwruAS42UjSXvtMYBV/gX0E+A9iJGmvfSawikxgxmPgPfaaAAHDnqDsQmA2UACZ3kXKhAJUKWihliUKkFJoRcuoCpAhoIalVhUgYxDHWmMSIEcgOfcWp2IL0vHN0zhinkAKaoTWLDRNoCdNE+jJcoEf1VNdHhBR9pYAAAAASUVORK5CYII=";
        
        private const string NonEmptyAnswerHint =
            " ;Always provide a valid structured response matching the schema (if you have no answer or an empty answer - please return default values instead)";

        private const string JpegScript = @"
ai.genContext({})
    .withJpeg(loadAttachment('image.png'));
";

        private const string PngScript = @"
ai.genContext({})
    .withPng(loadAttachment('image.Jpg'));
";

        private const string ImagePng = "image.png";
        private const string ImageJpg = "image.Jpg";

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { true })]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[]{ false })]
        public async Task CanUseImageWithTheWrongFormat(Options options, GenAiConfiguration config, bool withJpeg)
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
            config.GenAiTransformation = new GenAiTransformation { Script = withJpeg ? JpegScript : PngScript };

            await store.Maintenance.SendAsync(new AddGenAiOperation(config));

            var etl = Etl.WaitForEtlToComplete(store);

            var marker = new ImageDescription("None" + Guid.NewGuid(), false, new string[] { "None" });
            var markerArr = new ImageDescription[] { marker };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(markerArr), "items/1");
                using var file1 = new MemoryStream(Convert.FromBase64String(HeartPngBase64));

                session.Advanced.Attachments.Store("items/1", withJpeg ? ImagePng : ImageJpg, file1);
                await session.SaveChangesAsync();
            }
            Assert.True(etl.Wait(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 30)));

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.False(ValidateNonRefusalNotification(db));

            using (var session = store.OpenAsyncSession())
            {
                var item1 = await session.LoadAsync<Item>("items/1");
                Assert.NotNull(item1.ImageDescriptions);

                Assert.False(item1.ImageDescriptions.Any(d => d.Description == marker.Description));
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanUseImageActualName(Options options, GenAiConfiguration config)
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
            var markerArr = new ImageDescription[] { marker };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(markerArr), "items/1");
                using var file1 = new MemoryStream(Convert.FromBase64String(HeartPngBase64));

                session.Advanced.Attachments.Store("items/1", "image.png", file1);
                await session.SaveChangesAsync();
            }
            etl.Wait(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 30));

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            ValidateRefusalNotification(db, "was never loaded.");
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task ShouldNotifyOnInvalidImage(Options options, GenAiConfiguration config)
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
                await session.StoreAsync(new Item(markerArr), "items/1");
                var notValidBase64Att = Convert.FromBase64String(HeartPngBase64);
                notValidBase64Att[0] = 0xFF;
                using var file1 = new MemoryStream(notValidBase64Att);

                session.Advanced.Attachments.Store("items/1", "image.png", file1);
                await session.SaveChangesAsync();
            }
            etl.Wait(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 30));

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            ValidateRefusalNotification(db, "You uploaded an unsupported image.");
        }

        private static bool ValidateRefusalNotification(DocumentDatabase db,string msg)
        {
            using (db.NotificationCenter.GetStored(out var actions))
            {
                var jsonAlerts = actions.ToList();
                if (jsonAlerts.Count == 0)
                    return false;
                var bjro = jsonAlerts.First().Json;

                return bjro.TryGet("Details", out BlittableJsonReaderObject details) &&
                       details.TryGet("Errors", out BlittableJsonReaderArray errors) &&
                       errors.Length > 0 &&
                       errors.First().ToString()!.Contains(msg);
            }
        }

        private static bool ValidateNonRefusalNotification(DocumentDatabase db)
        {
            using (db.NotificationCenter.GetStored(out var actions))
            {
                var jsonAlerts = actions.ToList();
                if (jsonAlerts.Count == 0)
                    return false;
                var bjro = jsonAlerts.First().Json;

                return bjro.TryGet("Details", out BlittableJsonReaderObject details) &&
                       details.TryGet("Errors", out BlittableJsonReaderArray errors) &&
                       errors.Length > 0 &&
                       errors.First().IsNull();
            }
        }
    }
}

using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminTombstoneHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/tombstones/cleanup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task Cleanup()
        {
            var count = await Database.TombstoneCleaner.ExecuteCleanup();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Value");
                    writer.WriteInteger(count);

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/admin/tombstones/state", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public Task State()
        {
            var state = Database.TombstoneCleaner.GetState();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteArray(context, "Results", state, (w, c, v) =>
                    {
                        w.WriteStartObject();

                        w.WritePropertyName("Collection");
                        w.WriteString(v.Key);
                        w.WriteComma();
                        
                        w.WritePropertyName(nameof(v.Value.Component));
                        w.WriteString(v.Value.Component);
                        w.WriteComma();

                        w.WritePropertyName(nameof(v.Value.Etag));
                        w.WriteInteger(v.Value.Etag);

                        w.WriteEndObject();
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }
    }
}

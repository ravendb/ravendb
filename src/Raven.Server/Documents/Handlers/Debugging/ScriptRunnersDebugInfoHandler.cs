using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ScriptRunnersDebugInfoHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/script-runners", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task GetJSDebugInfo()
        {
            var detailed = GetBoolValueQueryString("detailed", required: false) ?? false;

            using (Database.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("ScriptRunners");

                    writer.WriteStartArray();
                    var first = true;
                    foreach (var runnerInfo in Database.Scripts.GetDebugInfo(detailed))
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;
                        using (var runnerInfoReader = context.ReadObject(runnerInfo, "runnerInfo"))
                            writer.WriteObject(runnerInfoReader);
                    }
                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }
    }
}

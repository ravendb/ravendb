using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminScriptRunnersDebugInfoHandler : RequestHandler
    {
        [RavenAction("/admin/debug/script-runners", "GET", AuthorizationStatus.Operator)]
        public async Task GetJSAdminDebugInfo()
        {
            var detailed = GetBoolValueQueryString("detailed", required: false) ?? false;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("ScriptRunners");

                    writer.WriteStartArray();
                    var first = true;
                    foreach (var runnerInfo in Server.AdminScripts.GetDebugInfo(detailed))
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
        }
    }
}

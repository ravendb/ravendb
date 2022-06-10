using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors
{
    internal class EtlHandlerProcessorForTestEtl : AbstractEtlHandlerProcessorForTestEtl<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public EtlHandlerProcessorForTestEtl([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetAndWriteEtlTestScriptResultAsync(DocumentsOperationContext context, BlittableJsonReaderObject testConfig)
        {
            var testScript = JsonDeserializationServer.TestRavenEtlScript(testConfig);

            using (RavenEtl.TestScript(testScript, RequestHandler.Database, ServerStore, context, out var testResult))
            {
                var result = (RavenEtlTestScriptResult)testResult;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    var defaultConventions = new DocumentConventions();

                    var djv = new DynamicJsonValue()
                    {
                        [nameof(result.Commands)] = new DynamicJsonArray(result.Commands.Select(x => x.ToJson(defaultConventions, context))),
                        [nameof(result.TransformationErrors)] = new DynamicJsonArray(result.TransformationErrors.Select(x => x.ToJson())),
                        [nameof(result.DebugOutput)] = new DynamicJsonArray(result.DebugOutput)
                    };

                    writer.WriteObject(context.ReadObject(djv, "et/raven/test"));
                }
            }
        }
    }
}

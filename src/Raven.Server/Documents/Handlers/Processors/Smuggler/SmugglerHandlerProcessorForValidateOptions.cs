using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
    internal class SmugglerHandlerProcessorForValidateOptions<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        internal SmugglerHandlerProcessorForValidateOptions(AbstractDatabaseRequestHandler<TOperationContext> requestHandler) : base(requestHandler)
        {

        }

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var blittableJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "");
                var options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);

                if (!string.IsNullOrEmpty(options.FileName) && options.FileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                    throw new InvalidOperationException($"{options.FileName} is invalid file name");

                if (string.IsNullOrEmpty(options.TransformScript))
                {
                    RequestHandler.NoContentStatus();
                    return;
                }

                try
                {
                    ScriptRunner.TryCompileScript(string.Format(@"
                    function execute(){{
                        {0}
                    }};", options.TransformScript));
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Incorrect transform script", e);
                }

                RequestHandler.NoContentStatus();
            }
        }
    }
}

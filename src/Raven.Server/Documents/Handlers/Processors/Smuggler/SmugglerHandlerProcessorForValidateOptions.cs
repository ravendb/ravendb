using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
    internal class SmugglerHandlerProcessorForValidateOptions<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        internal SmugglerHandlerProcessorForValidateOptions(AbstractDatabaseRequestHandler<TOperationContext> requestHandler, RavenConfiguration configuration) : base(requestHandler)
        {
            _engine = CreateJsEngine(configuration);
        }

        private static IScriptEngineChanges CreateJsEngine(RavenConfiguration configuration)
        {
            switch (configuration.JavaScript.EngineType)
            {
                case JavaScriptEngineType.Jint:
                    return new JintEngineEx(configuration);
                case JavaScriptEngineType.V8:
                    return V8EngineEx.GetPool(configuration).GetValue().Value;
                default:
                    throw new ArgumentOutOfRangeException();
            }
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
                    _engine.TryCompileScript(string.Format(@"
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

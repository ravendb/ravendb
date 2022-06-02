using System;
using Jint.Runtime;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide;
using Sparrow.Json;
using V8.Net;


namespace Raven.Server.Smuggler.Documents
{
    public class ServerSmugglerPatcher : SmugglerPatcher
    {
        public ServerSmugglerPatcher(DatabaseSmugglerOptions options, ServerStore server) : base(options, server.Server.AdminScripts)
        {
        }
    }

    public class DatabaseSmugglerPatcher : SmugglerPatcher
    {
        public DatabaseSmugglerPatcher(DatabaseSmugglerOptions options, DocumentDatabase database) : base(options, database.Scripts)
        {
        }
    }

    public abstract class SmugglerPatcher
    {
        private readonly DatabaseSmugglerOptions _options;
        private readonly ScriptRunnerCache _cache;
        private ISingleRun _run;

        protected SmugglerPatcher(DatabaseSmugglerOptions options, ScriptRunnerCache cache)
        {
            if (string.IsNullOrWhiteSpace(options.TransformScript))
                throw new InvalidOperationException("Cannot create a patcher with empty transform script.");
            _options = options;
            _cache = cache;
        }
        
        public Document Transform(Document document)
        {
            var ctx = document.Data._context;
            object translatedResult;
            using (document)
            {
                using (_run.ScriptEngineHandle.ChangeMaxStatements(_options.OptionsForTransformScript.MaxSteps))
                using (_run.ScriptEngineHandle.ChangeMaxDuration(_options.OptionsForTransformScript.MaxDuration))
                {
                    try
                    {
                        using (var result = _run.Run(ctx, null, "execute", new object[] { document }))
                        {
                            translatedResult = _run.Translate(result, ctx, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        }
                    }
                    catch (Client.Exceptions.Documents.Patching.JavaScriptException e)
                    {
                        if (e.InnerException is JintException innerExceptionJint && string.Equals(innerExceptionJint.Message, "skip", StringComparison.OrdinalIgnoreCase))
                            return null;
                        if (e.InnerException is V8Exception innerExceptionV8 && string.Equals(innerExceptionV8.Message, "skip", StringComparison.OrdinalIgnoreCase))
                            return null;

                        throw;
                    }
                }

                if (!(translatedResult is BlittableJsonReaderObject bjro))
                    return null;

                var cloned = document.Clone(ctx);
                using (cloned.Data)
                {
                    cloned.Data = bjro;
                }

                return cloned;
            }
        }

        public IDisposable Initialize()
        {
            var key = new PatchRequest(_options.TransformScript, PatchRequestType.Smuggler);
            return _cache.GetScriptRunner(key, readOnly: true, out _run);
        }
    }
}

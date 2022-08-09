using System;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Sparrow.Json;

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
        private ScriptRunner.SingleRun _run;

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
                using (_run.ScriptEngine.ChangeMaxStatements(_options.MaxStepsForTransformScript))
                {
                    try
                    {
                        using (ScriptRunnerResult result = _run.Run(ctx, null, "execute", new object[] { document }))
                        {
                            translatedResult = _run.Translate(result, ctx, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        }
                    }
                    catch (Client.Exceptions.Documents.Patching.JavaScriptException e)
                    {
                        if (e.InnerException is Jint.Runtime.JavaScriptException innerException && string.Equals(innerException.Message, "skip", StringComparison.OrdinalIgnoreCase))
                            return null;

                        throw;
                    }
                }

                if (!(translatedResult is BlittableJsonReaderObject bjro))
                    return null;

                return document.CloneWith(ctx, bjro);
            }
        }

        public IDisposable Initialize()
        {
            var key = new PatchRequest(_options.TransformScript, PatchRequestType.Smuggler);
            return _cache.GetScriptRunner(key, true, out _run);
        }
    }
}

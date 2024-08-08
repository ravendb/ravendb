using System;
using System.Collections.Generic;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public sealed class ServerSmugglerPatcher : SmugglerPatcher
    {
        public ServerSmugglerPatcher(DatabaseSmugglerOptions options, ServerStore server) : base(options, server.Server.AdminScripts)
        {
        }
    }

    public sealed class DatabaseSmugglerPatcher : SmugglerPatcher
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

        private readonly HashSet<string> _skippedDocumentIds = new(StringComparer.OrdinalIgnoreCase);

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
            using (document)
            {
                object translatedResult;
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
                        {
                            _skippedDocumentIds.Add(document.Id);
                            return null;
                        }

                        throw;
                    }
                }

                if (translatedResult is Document d)
                    return d.Clone(ctx);

                if (!(translatedResult is BlittableJsonReaderObject bjro))
                    return null;

                if (ctx.CachedProperties.NeedClearPropertiesCache())
                {
                    ctx.CachedProperties.Reset();
                }

                return document.CloneWith(ctx, bjro);
            }
        }

        public bool ShouldSkip(LazyStringValue documentId)
        {
            return _skippedDocumentIds.Count > 0 && _skippedDocumentIds.Contains(documentId);
        }

        public IDisposable Initialize()
        {
            _skippedDocumentIds.Clear();

            var key = new PatchRequest(_options.TransformScript, PatchRequestType.Smuggler);
            return _cache.GetScriptRunner(key, true, out _run);
        }
    }
}

using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public abstract class PatchConflict
    {
        protected readonly DocumentDatabase _database;
        protected readonly List<object> _docs = new List<object>();
        protected readonly DocumentConflict _fstDocumentConflict;
        protected readonly bool _hasTombstone;
        protected static readonly string TombstoneResolverValue = Guid.NewGuid().ToString();
        protected readonly Logger _logger;

        public static PatchConflict CreatePatchConflict(DocumentDatabase database, IReadOnlyList<DocumentConflict> docs)
        {
            switch (database.Configuration.JavaScript.EngineType)
            {
                case JavaScriptEngineType.Jint:
                    return new PatchConflictJint(database, docs);
                case JavaScriptEngineType.V8:
                    return new PatchConflictV8(database, docs);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        protected PatchConflict(DocumentDatabase database, IReadOnlyList<DocumentConflict> docs)
        {
            _logger = LoggingSource.Instance.GetLogger<PatchConflict>(database.Name);
            _database = database;
            foreach (var doc in docs)
            {
                if (doc.Doc != null)
                {
                    _docs.Add(doc);
                }
                else
                {
                    _hasTombstone = true;
                }
            }
            _fstDocumentConflict = docs[0];
        }

        public abstract bool TryResolveConflict(DocumentsOperationContext context, PatchRequest patch, out BlittableJsonReaderObject resolved);

        protected bool TryResolveConflictInternal<T>(ScriptRunnerResult<T> result, out bool tryResolveConflict)
        where T : struct, IJsHandle<T>
        {
            if (result.IsNull)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Conflict resolution script for {_fstDocumentConflict.Collection} collection declined to resolve the conflict for {_fstDocumentConflict.Id ?? _fstDocumentConflict.LowerId}");
                }

                tryResolveConflict = false;
                return true;
            }

            if (result.StringValue == TombstoneResolverValue)
            {
                tryResolveConflict = true;
                return true;
            }

            tryResolveConflict = default;
            return false;
        }

        protected void TryAddMetadata<T>(ScriptRunnerResult<T> result, SingleRun<T> run)
            where T : struct, IJsHandle<T>
        {
            using (var instance = result.GetOrCreate(Constants.Documents.Metadata.Key)) // not disposing as we use the cached value
            {
                // if user didn't specify it, we'll take it from the first doc
                // we cannot change collections here anyway, anything else, the 
                // user need to merge on their own
                if (instance.SetProperty(Constants.Documents.Metadata.Collection, run.ScriptEngineHandle.CreateValue(_fstDocumentConflict.Collection.ToString())) ==
                    false)
                {
                    _logger.Info(
                        $"Conflict resolution script for {_fstDocumentConflict.Collection} collection failed to set property collection: the conflict for {_fstDocumentConflict.Id ?? _fstDocumentConflict.LowerId}");
                }
            }
        }
    }

    public class PatchConflictV8 : PatchConflict
    {
        public PatchConflictV8(DocumentDatabase database, IReadOnlyList<DocumentConflict> docs) : base(database, docs)
        {
        }

        public override bool TryResolveConflict(DocumentsOperationContext context, PatchRequest patch, out BlittableJsonReaderObject resolved)
        {
            using (_database.Scripts.GetScriptRunnerV8(patch, readOnly: false, out SingleRunV8 run))
            using (ScriptRunnerResult<JsHandleV8> result = run.Run(context, context, "resolve", documentId: null,
                       args: new object[] { _docs, _hasTombstone, TombstoneResolverValue }))
            {
                resolved = null;
                if (TryResolveConflictInternal(result, out bool tryResolveConflict))
                    return tryResolveConflict;

                TryAddMetadata(result, run);

                resolved = result.TranslateToObject(context, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                return true;
            }
        }
    }

    public class PatchConflictJint : PatchConflict
    {
        public PatchConflictJint(DocumentDatabase database, IReadOnlyList<DocumentConflict> docs) : base(database, docs)
        {
        }

        public override bool TryResolveConflict(DocumentsOperationContext context, PatchRequest patch, out BlittableJsonReaderObject resolved)
        {
            using (_database.Scripts.GetScriptRunnerJint(patch, readOnly: false, out SingleRunJint run))
            using (ScriptRunnerResult<JsHandleJint> result = run.Run(context, context, "resolve", documentId: null,
                       args: new object[] { _docs, _hasTombstone, TombstoneResolverValue }))
            {
                resolved = null;
                if (TryResolveConflictInternal(result, out bool tryResolveConflict))
                    return tryResolveConflict;

                TryAddMetadata(result, run);

                resolved = result.TranslateToObject(context, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                return true;
            }
        }
    }
}

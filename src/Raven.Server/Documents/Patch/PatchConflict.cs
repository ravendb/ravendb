using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Raven.Server.Config.Categories;
using PatchJint = Raven.Server.Documents.Patch.Jint;
using PatchV8 = Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Patch
{
    public class PatchConflict
    {
        protected readonly IJavaScriptOptions _jsOptions;
        protected readonly DocumentDatabase _database;
        protected readonly List<object> _docs = new List<object>();
        protected readonly DocumentConflict _fstDocumentConflict;
        protected readonly bool _hasTombstone;
        protected static readonly string TombstoneResolverValue = Guid.NewGuid().ToString();
        protected readonly Logger _logger;


        public static PatchConflict CreatePatchConflict(DocumentDatabase database, IReadOnlyList<DocumentConflict> docs)
        {
            var jsOptions = database.JsOptions;
            return new PatchConflict(database, docs);
        }
        
        public PatchConflict(DocumentDatabase database, IReadOnlyList<DocumentConflict> docs)
        {
            _logger = LoggingSource.Instance.GetLogger<PatchConflict>(database.Name);
            _database = database;
            _jsOptions = database.JsOptions;

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

        public bool TryResolveConflict(DocumentsOperationContext context, PatchRequest patch, out BlittableJsonReaderObject resolved)
        {
            using (_database.Scripts.GetScriptRunner(_jsOptions, patch, false, out var run))
            using (var result = run.Run(context, context, "resolve", new object[] {_docs, _hasTombstone, TombstoneResolverValue}))
            {
                if (result.IsNull)
                {
                    resolved = null;
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Conflict resolution script for {_fstDocumentConflict.Collection} collection declined to resolve the conflict for {_fstDocumentConflict.Id ?? _fstDocumentConflict.LowerId}");
                    }

                    return false;
                }

                if (result.StringValue == TombstoneResolverValue)
                {
                    resolved = null;
                    return true;
                }

                using (var instance = result.GetOrCreate(Constants.Documents.Metadata.Key)) // not disposing as we use the cached value
                {
                    // if user didn't specify it, we'll take it from the first doc
                    // we cannot change collections here anyway, anything else, the 
                    // user need to merge on their own
                    if (instance.SetProperty(Constants.Documents.Metadata.Collection, result.EngineHandle.CreateValue(_fstDocumentConflict.Collection.ToString())) == false)
                    {
                        _logger.Info(
                            $"Conflict resolution script for {_fstDocumentConflict.Collection} collection failed to set property collection: the conflict for {_fstDocumentConflict.Id ?? _fstDocumentConflict.LowerId}");
                    }
                }

                resolved = result.TranslateToObject(context, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                return true;
            }
        }
    }
}

﻿using System;
using System.Collections.Generic;
using Jurassic;
using Jurassic.Library;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public class PatchConflict 
    {
        private readonly DocumentDatabase _database;
        private readonly List<object> _docs = new List<object>();
        private readonly DocumentConflict _fstDocumentConflict;
        private readonly bool _hasTombstone;
        private static readonly string TombstoneResolverValue = Guid.NewGuid().ToString();
        private readonly Logger _logger;

        public PatchConflict(DocumentDatabase database, IReadOnlyList<DocumentConflict> docs)
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

        public bool TryResolveConflict(DocumentsOperationContext context, PatchRequest patch, out BlittableJsonReaderObject resolved)
        {
            using(_database.Scripts.GetScriptRunner(patch, out var run))
            {
                run.ReadOnly = true;
                var result = run.Run(context, "resolve", new object[] {_docs, _hasTombstone, TombstoneResolverValue});
                if (result.IsNull)
                {
                    resolved = null;
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Conflict resolution script for {_fstDocumentConflict.Collection} collection declined to resolve the conflict for {_fstDocumentConflict.LowerId}");
                    }
                    return false;
                }
                if (result.Value is string s && s == TombstoneResolverValue)
                {
                    resolved = null;
                    return true;
                }
                var instance = result.Get(Constants.Documents.Metadata.Key);
                // if user didn't specify it, we'll take it from the first doc
                // we cannot change collections here anyway, anything else, the 
                // user need to merge on their own
                instance[Constants.Documents.Metadata.Collection] = _fstDocumentConflict.Collection;
                resolved = result.Translate<BlittableJsonReaderObject>(context,
                    BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                return true;
            }
        }
    }
}

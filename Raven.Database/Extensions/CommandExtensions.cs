//-----------------------------------------------------------------------
// <copyright file="CommandExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;
namespace Raven.Database.Extensions
{
    public static class CommandExtensions
    {
        public static void Execute(this ICommandData self, DocumentDatabase database)
        {
            Execute(self, database, null);
        }

        public static BatchResult ExecuteBatch(this ICommandData self, DocumentDatabase database, IEnumerable<string> participatingIds = null)
        {
            var batchResult = new BatchResult();

            Execute(self, database, batchResult, participatingIds);

            batchResult.Method = self.Method;
            batchResult.Key = self.Key;
            batchResult.Etag = self.Etag;
            batchResult.Metadata = self.Metadata;
            batchResult.AdditionalData = self.AdditionalData;

            return batchResult;
        }

        private static void Execute(ICommandData self, DocumentDatabase database, BatchResult batchResult, IEnumerable<string> participatingIds = null)
        {
            var deleteCommandData = self as DeleteCommandData;
            if (deleteCommandData != null)
            {
                var result = database.Documents.Delete(deleteCommandData.Key, deleteCommandData.Etag, deleteCommandData.TransactionInformation,participatingIds);

                if (batchResult != null)
                    batchResult.Deleted = result;

                return;
            }

            var putCommandData = self as PutCommandData;
            if (putCommandData != null)
            {
                var putResult = database.Documents.Put(putCommandData.Key, putCommandData.Etag, putCommandData.Document, putCommandData.Metadata, putCommandData.TransactionInformation, participatingIds);
                putCommandData.Etag = putResult.ETag;
                putCommandData.Key = putResult.Key;

                return;
            }

            var patchCommandData = self as PatchCommandData;
            if (patchCommandData != null)
            {
                var result = database.Patches.ApplyPatch(patchCommandData.Key, patchCommandData.Etag,
                                                 patchCommandData.Patches, patchCommandData.PatchesIfMissing, patchCommandData.Metadata,
                                                 patchCommandData.TransactionInformation,
                                                 skipPatchIfEtagMismatch: patchCommandData.SkipPatchIfEtagMismatch, participatingIds: participatingIds);

                if (batchResult != null)
                    batchResult.PatchResult = result.PatchResult;

                var doc = database.Documents.Get(patchCommandData.Key, patchCommandData.TransactionInformation);
                if (doc != null)
                {
                    database.TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() =>
                    {
                        patchCommandData.Metadata = doc.Metadata;
                        patchCommandData.Etag = doc.Etag;
                    });
                }
                return;
            }

            var advPatchCommandData = self as ScriptedPatchCommandData;
            if (advPatchCommandData != null)
            {
                var result = database.Patches.ApplyPatch(advPatchCommandData.Key, advPatchCommandData.Etag,
                                                 advPatchCommandData.Patch, advPatchCommandData.PatchIfMissing, advPatchCommandData.Metadata,
                                                 advPatchCommandData.TransactionInformation, advPatchCommandData.DebugMode, participatingIds);

                if (batchResult != null)
                    batchResult.PatchResult = result.Item1.PatchResult;

                advPatchCommandData.AdditionalData = new RavenJObject { { "Debug", new RavenJArray(result.Item2) } };
                if(advPatchCommandData.DebugMode)
                {
                    advPatchCommandData.AdditionalData["Document"] = result.Item1.Document;
                    advPatchCommandData.AdditionalData["Actions"] = result.Item1.DebugActions;
                    return;
                }

                var doc = database.Documents.Get(advPatchCommandData.Key, advPatchCommandData.TransactionInformation);
                if (doc != null)
                {
                    database.TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() =>
                    {
                        advPatchCommandData.Metadata = doc.Metadata;
                        advPatchCommandData.Etag = doc.Etag;
                    });
                }
            }
        }
    }
}

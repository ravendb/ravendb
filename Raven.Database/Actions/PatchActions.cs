// -----------------------------------------------------------------------
//  <copyright file="PatchActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class PatchActions : ActionsBase
    {
        public PatchActions(DocumentDatabase database, IUuidGenerator uuidGenerator, ILog log)
            : base(database, uuidGenerator, log)
        {
        }

        public PatchResultData ApplyPatch(string docId, Etag etag, PatchRequest[] patchDoc,
                                  TransactionInformation transactionInformation, bool debugMode = false, IEnumerable<string> participatingIds = null)
        {
            if (docId == null)
                throw new ArgumentNullException("docId");
            return ApplyPatchInternal(docId, etag, transactionInformation,
                                      jsonDoc => new JsonPatcher(jsonDoc.ToJson()).Apply(patchDoc),
                                      () => null, () => null, () => null, debugMode, participatingIds: participatingIds);
        }

        public PatchResultData ApplyPatch(string docId, Etag etag,
                                          PatchRequest[] patchExistingDoc, PatchRequest[] patchDefaultDoc, RavenJObject defaultMetadata,
                                          TransactionInformation transactionInformation, bool debugMode = false, bool skipPatchIfEtagMismatch = false,
                                          IEnumerable<string> participatingIds = null)
        {
            if (docId == null)
                throw new ArgumentNullException("docId");
            return ApplyPatchInternal(docId, etag, transactionInformation,
                                      jsonDoc => new JsonPatcher(jsonDoc.ToJson()).Apply(patchExistingDoc),
                                      () =>
                                      {
                                          if (patchDefaultDoc == null || patchDefaultDoc.Length == 0)
                                              return null;

                                          var jsonDoc = new RavenJObject();
                                          jsonDoc[Constants.Metadata] = defaultMetadata.CloneToken() ?? new RavenJObject();
                                          return new JsonPatcher(jsonDoc).Apply(patchDefaultDoc);
                                      },
                                      () => null,
                                      () => null,
                                      debugMode,
                                      skipPatchIfEtagMismatch,
                                      participatingIds);
        }

        private PatchResultData ApplyPatchInternal(string docId, Etag etag,
                                       TransactionInformation transactionInformation,
                                       Func<JsonDocument, RavenJObject> patcher,
                                       Func<RavenJObject> patcherIfMissing,
                                       Func<IList<JsonDocument>> getDocsCreatedInPatch,
                                       Func<RavenJObject> getDebugActions,
                                       bool debugMode,
                                       bool skipPatchIfEtagMismatch = false,
                                       IEnumerable<string> participatingIds = null)
        {
            if (docId == null) throw new ArgumentNullException("docId");
            docId = docId.Trim();
            var result = new PatchResultData
            {
                PatchResult = PatchResult.Patched
            };

            bool shouldRetry = false;
            int retries = 128;
            Random rand = null;
            do
            {
                var doc = Database.Documents.Get(docId, transactionInformation);
                if (Log.IsDebugEnabled)
                    Log.Debug(() => string.Format("Preparing to apply patch on ({0}). Document found?: {1}.", docId, doc != null));

                if (etag != null && doc != null && doc.Etag != etag)
                {
                    Debug.Assert(doc.Etag != null);

                    if (skipPatchIfEtagMismatch)
                    {
                        result.PatchResult = PatchResult.Skipped;
                        return result;
                    }

                    if (Log.IsDebugEnabled)
                        Log.Debug(() => string.Format("Got concurrent exception while tried to patch the following document ID: {0}", docId));
                    throw new ConcurrencyException("Could not patch document '" + docId + "' because non current etag was used")
                    {
                        ActualETag = doc.Etag,
                        ExpectedETag = etag,
                    };
                }
                var documentBeforePatching = doc != null ? doc.ToJson().CloneToken() : null;

                var jsonDoc = (doc != null ? patcher(doc) : patcherIfMissing());

                if (jsonDoc == null)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug(() => string.Format("Preparing to apply patch on ({0}). DocumentDoesNotExists.", docId));
                    result.PatchResult = PatchResult.DocumentDoesNotExists;
                }
                else
                {
                    if (debugMode)
                    {
                        result.Document = jsonDoc;
                        result.PatchResult = PatchResult.Tested;
                        result.DebugActions = getDebugActions();
                    }
                    else
                    {
                        try
                        {
                            var notModified = false;

                            if (doc == null)
                                Database.Documents.Put(docId, null, jsonDoc, jsonDoc.Value<RavenJObject>(Constants.Metadata), transactionInformation, participatingIds);
                            else
                            {
                                if (IsNotModified(jsonDoc.CloneToken(), documentBeforePatching))
                                    notModified = true;
                                else
                                    Database.Documents.Put(doc.Key, (doc.Etag), jsonDoc, jsonDoc.Value<RavenJObject>(Constants.Metadata), transactionInformation, participatingIds);
                            }

                            var docsCreatedInPatch = getDocsCreatedInPatch();
                            if (docsCreatedInPatch != null && docsCreatedInPatch.Count > 0)
                            {
                                foreach (var docFromPatch in docsCreatedInPatch)
                                {
                                    Database.Documents.Put(docFromPatch.Key, docFromPatch.Etag, docFromPatch.DataAsJson,
                                        docFromPatch.Metadata, transactionInformation, participatingIds);
                                }
                            }
                            shouldRetry = false;
                            result.PatchResult = notModified ? PatchResult.NotModified : PatchResult.Patched;
                        }
                        catch (ConcurrencyException)
                        {
                            if (TransactionalStorage.IsAlreadyInBatch)
                                throw;
                            if (retries-- > 0)
                            {
                                shouldRetry = true;
                                if (rand == null)
                                    rand = new Random();
                                Thread.Sleep(rand.Next(5, Math.Max(retries * 2, 10)));
                                continue;
                            }

                            throw;
                        }
                    }
                }

                if (shouldRetry == false)
                    WorkContext.ShouldNotifyAboutWork(() => "PATCH " + docId);

            } while (shouldRetry);

            return result;
        }

        private static bool IsNotModified(RavenJToken patchedDocClone, RavenJToken existingDocClone)
        {
            patchedDocClone.Value<RavenJObject>(Constants.Metadata).Remove(Constants.LastModified);
            existingDocClone.Value<RavenJObject>(Constants.Metadata).Remove(Constants.LastModified);

            return RavenJToken.DeepEquals(patchedDocClone, existingDocClone);
        }

        public Tuple<PatchResultData, List<string>> ApplyPatch(string docId, Etag etag, ScriptedPatchRequest patch,
                                                       TransactionInformation transactionInformation, bool debugMode = false)
        {
            ScriptedJsonPatcher scriptedJsonPatcher = null;
            DefaultScriptedJsonPatcherOperationScope scope = null;
            try
            {
                var applyPatchInternal = ApplyPatchInternal(docId, etag, transactionInformation,
                    jsonDoc =>
                    {
                        scope = new DefaultScriptedJsonPatcherOperationScope(Database, debugMode);
                        scriptedJsonPatcher = new ScriptedJsonPatcher(Database);
                        return scriptedJsonPatcher.Apply(scope, jsonDoc.ToJson(), patch, jsonDoc.SerializedSizeOnDisk, jsonDoc.Key);
                    },
                    () => null,
                    () =>
                    {
                        if (scope == null)
                            return null;
                        return scope
                            .GetPutOperations()
                            .ToList();
                    }, 
                    () =>
                    {
                        if (scope == null)
                            return null;

                        return scope.DebugActions;
                    },
                    debugMode);

                return Tuple.Create(applyPatchInternal, scriptedJsonPatcher == null ? new List<string>() : scriptedJsonPatcher.Debug);
            }
            finally
            {
                if (scope != null)
                    scope.Dispose();
            }
        }

        public Tuple<PatchResultData, List<string>> ApplyPatch(string docId, Etag etag,
                                                               ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata,
                                                               TransactionInformation transactionInformation, bool debugMode = false, IEnumerable<string> participatingIds = null)
        {
            ScriptedJsonPatcher scriptedJsonPatcher = null;
            DefaultScriptedJsonPatcherOperationScope scope = null;

            try
            {
                var applyPatchInternal = ApplyPatchInternal(docId, etag, transactionInformation,
                    jsonDoc =>
                    {
                        scope = scope ?? new DefaultScriptedJsonPatcherOperationScope(Database, debugMode);
                        scriptedJsonPatcher = new ScriptedJsonPatcher(Database);
                        return scriptedJsonPatcher.Apply(scope, jsonDoc.ToJson(), patchExisting, jsonDoc.SerializedSizeOnDisk, jsonDoc.Key);
                    },
                    () =>
                    {
                        if (patchDefault == null)
                            return null;

                        scope = scope ?? new DefaultScriptedJsonPatcherOperationScope(Database, debugMode);

                        scriptedJsonPatcher = new ScriptedJsonPatcher(Database);
                        var jsonDoc = new RavenJObject();
                        jsonDoc[Constants.Metadata] = defaultMetadata.CloneToken() ?? new RavenJObject();
                        return scriptedJsonPatcher.Apply(scope, jsonDoc, patchDefault, 0, docId);
                    },
                    () =>
                    {
                        if (scope == null)
                            return null;
                        return scope
                            .GetPutOperations()
                            .ToList();
                    },
                    () =>
                    {
                        if (scope == null)
                            return null;

                        return scope.DebugActions;
                    },
                    debugMode, participatingIds: participatingIds);
                return Tuple.Create(applyPatchInternal, scriptedJsonPatcher == null ? new List<string>() : scriptedJsonPatcher.Debug);
            }
            finally
            {
                if (scope != null)
                    scope.Dispose();
            }
        }
    }
}

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
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class PatchActions : ActionsBase
    {
        public PatchActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

        public PatchResultData ApplyPatch(string docId, Etag etag, PatchRequest[] patchDoc,
                                  TransactionInformation transactionInformation, bool debugMode = false)
        {
            if (docId == null)
                throw new ArgumentNullException("docId");
            return ApplyPatchInternal(docId, etag, transactionInformation,
                                      jsonDoc => new JsonPatcher(jsonDoc.ToJson()).Apply(patchDoc),
                                      () => null, () => null, debugMode);
        }

        public PatchResultData ApplyPatch(string docId, Etag etag,
                                          PatchRequest[] patchExistingDoc, PatchRequest[] patchDefaultDoc, RavenJObject defaultMetadata,
                                          TransactionInformation transactionInformation, bool debugMode = false)
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
                                          jsonDoc[Constants.Metadata] = defaultMetadata ?? new RavenJObject();
                                          return new JsonPatcher(jsonDoc).Apply(patchDefaultDoc);
                                      },
                                      () => null, debugMode);
        }

        private PatchResultData ApplyPatchInternal(string docId, Etag etag,
                                       TransactionInformation transactionInformation,
                                       Func<JsonDocument, RavenJObject> patcher,
                                       Func<RavenJObject> patcherIfMissing,
                                       Func<IList<JsonDocument>> getDocsCreatedInPatch,
                                       bool debugMode)
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
                Log.Debug(() => string.Format("Preparing to apply patch on ({0}). Document found?: {1}.", docId, doc != null));

                if (etag != null && doc != null && doc.Etag != etag)
                {
                    Debug.Assert(doc.Etag != null);
                    Log.Debug(() => string.Format("Got concurrent exception while tried to patch the following document ID: {0}", docId));
                    throw new ConcurrencyException("Could not patch document '" + docId + "' because non current etag was used")
                    {
                        ActualETag = doc.Etag,
                        ExpectedETag = etag,
                    };
                }

                var jsonDoc = (doc != null ? patcher(doc) : patcherIfMissing());
                if (jsonDoc == null)
                {
                    Log.Debug(() => string.Format("Preparing to apply patch on ({0}). DocumentDoesNotExists.", docId));
                    result.PatchResult = PatchResult.DocumentDoesNotExists;
                }
                else
                {
                    if (debugMode)
                    {
                        result.Document = jsonDoc;
                        result.PatchResult = PatchResult.Tested;
                    }
                    else
                    {
                        try
                        {
                            Database.Documents.Put(doc == null ? docId : doc.Key, (doc == null ? null : doc.Etag), jsonDoc, jsonDoc.Value<RavenJObject>(Constants.Metadata), transactionInformation);

                            var docsCreatedInPatch = getDocsCreatedInPatch();
                            if (docsCreatedInPatch != null && docsCreatedInPatch.Count > 0)
                            {
                                foreach (var docFromPatch in docsCreatedInPatch)
                                {
                                    Database.Documents.Put(docFromPatch.Key, docFromPatch.Etag, docFromPatch.DataAsJson,
                                        docFromPatch.Metadata, transactionInformation);
                                }
                            }
                            shouldRetry = false;
                            result.PatchResult = PatchResult.Patched;
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

        public Tuple<PatchResultData, List<string>> ApplyPatch(string docId, Etag etag, ScriptedPatchRequest patch,
                                                       TransactionInformation transactionInformation, bool debugMode = false)
        {
            ScriptedJsonPatcher scriptedJsonPatcher = null;
            var applyPatchInternal = ApplyPatchInternal(docId, etag, transactionInformation,
                jsonDoc =>
                {
                    scriptedJsonPatcher = new ScriptedJsonPatcher(Database);
                    return scriptedJsonPatcher.Apply(jsonDoc.ToJson(), patch, jsonDoc.SerializedSizeOnDisk, jsonDoc.Key);
                },
                () => null,
                () =>
                {
                    if (scriptedJsonPatcher == null)
                        return null;
                    return scriptedJsonPatcher
                        .GetPutOperations()
                        .ToList();
                }, debugMode);
            return Tuple.Create(applyPatchInternal, scriptedJsonPatcher == null ? new List<string>() : scriptedJsonPatcher.Debug);
        }

        public Tuple<PatchResultData, List<string>> ApplyPatch(string docId, Etag etag,
                                                               ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata,
                                                               TransactionInformation transactionInformation, bool debugMode = false)
        {
            ScriptedJsonPatcher scriptedJsonPatcher = null;
            var applyPatchInternal = ApplyPatchInternal(docId, etag, transactionInformation,
                jsonDoc =>
                {
                    scriptedJsonPatcher = new ScriptedJsonPatcher(Database);
                    return scriptedJsonPatcher.Apply(jsonDoc.ToJson(), patchExisting, jsonDoc.SerializedSizeOnDisk, jsonDoc.Key);
                },
                () =>
                {
                    if (patchDefault == null)
                        return null;

                    scriptedJsonPatcher = new ScriptedJsonPatcher(Database);
                    var jsonDoc = new RavenJObject();
                    jsonDoc[Constants.Metadata] = defaultMetadata ?? new RavenJObject();
                    return scriptedJsonPatcher.Apply(new RavenJObject(), patchDefault, 0, docId);
                },
                () =>
                {
                    if (scriptedJsonPatcher == null)
                        return null;
                    return scriptedJsonPatcher
                        .GetPutOperations()
                        .ToList();
                }, debugMode);
            return Tuple.Create(applyPatchInternal, scriptedJsonPatcher == null ? new List<string>() : scriptedJsonPatcher.Debug);
        }



    }
}
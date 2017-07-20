using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Database.Indexing;

namespace Raven.Database.Tasks
{
    public class TouchReferenceDocumentIfChangedTask : DatabaseTask
    {
        private static readonly ILog logger = LogManager.GetCurrentClassLogger();
        private IDictionary<string, Etag> ReferencesToCheck { get; set; }

        public TouchReferenceDocumentIfChangedTask(int indexId) : base(indexId)
        {
            ReferencesToCheck = new Dictionary<string, Etag>(StringComparer.OrdinalIgnoreCase);
        }

        public override int NumberOfKeys
        {
            get
            {
                if (ReferencesToCheck == null)
                    return 0;
                return ReferencesToCheck.Count;
            }
        }

        public override string ToString()
        {
            return string.Format("Index: {0}, References Count: {1}, References: {2}",
                Index, ReferencesToCheck.Count, string.Join(", ", ReferencesToCheck.Keys));
        }

        public override bool SeparateTasksByIndex
        {
            get { return false; }
        }

        public override void Merge(DatabaseTask task)
        {
            var t = (TouchReferenceDocumentIfChangedTask)task;

            foreach (var kvp in t.ReferencesToCheck)
            {
                Etag etag;
                if (ReferencesToCheck.TryGetValue(kvp.Key, out etag) == false)
                {
                    ReferencesToCheck[kvp.Key] = kvp.Value;
                }
                else
                {
                    ReferencesToCheck[kvp.Key] = etag.CompareTo(kvp.Value) < 0 ? etag : kvp.Value;
                }
            }
        }

        public override void Execute(WorkContext context)
        {
            if (logger.IsDebugEnabled)
            {
                logger.Debug("Going to touch the following documents (LoadDocument references, need to check for concurrent transactions): {0}",
                    string.Join(", ", ReferencesToCheck));
            }

            using (context.Database.TransactionalStorage.DisableBatchNesting())
            {
                var docsToTouch = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var collectionsAndEtags = new Dictionary<string, Etag>(StringComparer.OrdinalIgnoreCase);

                context.TransactionalStorage.Batch(accessor =>
                {
                    foreach (var kvp in ReferencesToCheck)
                    {
                        var doc = accessor.Documents.DocumentMetadataByKey(kvp.Key);

                        if (doc == null)
                        {
                            if (logger.IsDebugEnabled)
                                logger.Debug("Cannot touch {0}, non existent document", kvp.Key);
                            continue;
                        }
                        if (doc.Etag == kvp.Value)
                        {
                            if (logger.IsDebugEnabled)
                                logger.Debug("Don't need to touch {0}, etag {1} is the same as when we last saw it", kvp.Key, doc.Etag);
                            continue;
                        }

                        var ravenDeleteMarker = doc.Metadata.Value<string>("Raven-Delete-Marker");
                        bool markerValue;
                        if (ravenDeleteMarker != null && bool.TryParse(ravenDeleteMarker, out markerValue) && markerValue)
                        {
                            if (logger.IsDebugEnabled)
                                logger.Debug("Don't need to touch {0}, etag {1} is illegal, probably it's a missing document", kvp.Key, doc.Etag);
                            continue;
                        }

                        docsToTouch.Add(kvp.Key);

                        var entityName = doc.Metadata.Value<string>(Constants.RavenEntityName);

                        if (string.IsNullOrEmpty(entityName))
                            continue;

                        Etag highestEtagInCollection;

                        if (collectionsAndEtags.TryGetValue(entityName, out highestEtagInCollection) == false || doc.Etag.CompareTo(highestEtagInCollection) > 0)
                        {
                            collectionsAndEtags[entityName] = doc.Etag;
                        }
                    }
                });

                var globalSw = Stopwatch.StartNew();
                var alreadyTouched = new List<string>();
                var totalTouchCount = 0;
                while (docsToTouch.Count > 0)
                {
                    Stopwatch sp;
                    using (context.Database.DocumentLock.Lock())
                    {
                        sp = Stopwatch.StartNew();
                        context.TransactionalStorage.Batch(accessor =>
                        {
                            foreach (var doc in docsToTouch)
                            {
                                if (sp.ElapsedMilliseconds > 500)
                                {
                                    sp.Stop();
                                    return;
                                }
                                    
                                alreadyTouched.Add(doc);

                                try
                                {
                                    Etag preTouchEtag;
                                    Etag afterTouchEtag;
                                    accessor.Documents.TouchDocument(doc, out preTouchEtag, out afterTouchEtag);
                                    totalTouchCount++;
                                }
                                catch (ConcurrencyException)
                                {
                                    logger.Info("Concurrency exception when touching {0}", doc);
                                }

                                var internalTouchCount = context.Database.Indexes.CheckReferenceBecauseOfDocumentUpdate(doc, accessor);
                                totalTouchCount += internalTouchCount;
                            }
                        });
                    }

                    foreach (var docToRemove in alreadyTouched)
                    {
                        docsToTouch.Remove(docToRemove);
                    }

                    if (sp.ElapsedMilliseconds > 500)
                        Thread.Sleep(0); // we let other threads access the lock if needed

                    if (logger.IsDebugEnabled)
                    {
                        logger.Debug($"Touched {totalTouchCount:#,#;;0} documents, " +
                                     $"took: {sp.ElapsedMilliseconds:0.#,#;;0}ms");
                    }

                    alreadyTouched.Clear();
                }

                if (globalSw.ElapsedMilliseconds > 3000)
                {
                    logger.Warn($"Documents to touch: {alreadyTouched.Count:#,#;;0}, " +
                                 $"touched {totalTouchCount:#,#;;0} documents," +
                                 $"took: {globalSw.ElapsedMilliseconds:0.#,#;;0}ms");
                }

                foreach (var collectionEtagPair in collectionsAndEtags)
                {
                    context.Database.LastCollectionEtags.Update(collectionEtagPair.Key, collectionEtagPair.Value);
                }
            }
        }

        public override DatabaseTask Clone()
        {
            return new TouchReferenceDocumentIfChangedTask(Index)
            {
                ReferencesToCheck = new Dictionary<string, Etag>(ReferencesToCheck, StringComparer.OrdinalIgnoreCase)
            };
        }

        public void UpdateReferenceToCheck(KeyValuePair<string, Etag> doc)
        {
            Etag etag;
            if (ReferencesToCheck.TryGetValue(doc.Key, out etag) == false)
            {
                ReferencesToCheck[doc.Key] = doc.Value;
                return;
            }

            if (etag == doc.Value)
                return;

            ReferencesToCheck[doc.Key] = Etag.InvalidEtag; // different etags, force a touch
        }

        public IEnumerable<string> GetReferencesForDebug()
        {
            return ReferencesToCheck.Select(x => x.Key + ":" + x.Value);
        }
    }
}

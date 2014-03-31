using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Database.Indexing;
using Raven.Database.Util;

namespace Raven.Database.Tasks
{
    public class TouchMissingReferenceDocumentTask : DatabaseTask
    {
        private static readonly ILog logger = LogManager.GetCurrentClassLogger();
		public IDictionary<string, HashSet<string>> MissingReferences { get; set; }

        public override string ToString()
        {
            return string.Format("Index: {0}, MissingReferences: {1}", Index, string.Join(", ", MissingReferences.Keys));
        }


        public override bool SeparateTasksByIndex
        {
            get { return false; }
        }

        public override void Merge(DatabaseTask task)
        {
            var t = (TouchMissingReferenceDocumentTask)task;

            foreach (var kvp in t.MissingReferences)
            {
                HashSet<string> set;
                if (MissingReferences.TryGetValue(kvp.Key, out set) == false)
                {
                    MissingReferences[kvp.Key] = kvp.Value;
                }
                else
                {
                    set.UnionWith(kvp.Value);
                }
            }
        }

        public override void Execute(WorkContext context)
        {
            if (logger.IsDebugEnabled)
            {
                logger.Debug("Going to touch the following documents (missing references, need to check for concurrent transactions): {0}",
                    string.Join(", ", MissingReferences));
            }
          
            context.TransactionalStorage.Batch(accessor =>
            {
                foreach (var docWithMissingRef in MissingReferences)
                {
                    foreach (var index in context.IndexStorage.Indexes)
                    {
                        var set = context.DoNotTouchAgainIfMissingReferences.GetOrAdd(index, _ => new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase));
                        set.Add(docWithMissingRef.Key);
                    }

	                bool foundReference = false;

					using (context.TransactionalStorage.DisableBatchNesting())
					{
						context.TransactionalStorage.Batch(freshAccessor =>
						{
							foreach (var missingRef in docWithMissingRef.Value)
							{
								var doc = freshAccessor.Documents.DocumentMetadataByKey(missingRef, null);

								if (doc == null) 
									continue;
								
								foundReference = true;
								break;
							}
						});
					}

					if(foundReference == false)
						continue;

                    try
                    {
                        using (context.Database.DocumentLock.Lock())
                        {
                            Etag preTouchEtag;
                            Etag afterTouchEtag;
                            accessor.Documents.TouchDocument(docWithMissingRef.Key, out preTouchEtag, out afterTouchEtag);
                        }
                    }
                    catch (ConcurrencyException)
                    {
                    }
                }
            });
        }

        public override DatabaseTask Clone()
        {
            return new TouchMissingReferenceDocumentTask
            {
                Index = Index,
				MissingReferences = new Dictionary<string, HashSet<string>>(MissingReferences)
            };
        }
    }
}

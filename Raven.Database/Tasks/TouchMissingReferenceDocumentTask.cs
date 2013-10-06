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
    public class TouchMissingReferenceDocumentTask : Task
    {
        private static readonly ILog logger = LogManager.GetCurrentClassLogger();
        public HashSet<string> Keys { get; set; }

        public override string ToString()
        {
            return string.Format("Index: {0}, Keys: {1}", Index, string.Join(", ", Keys));
        }

        public override void Merge(Task task)
        {
            var t = (TouchMissingReferenceDocumentTask)task;
            Keys.UnionWith(t.Keys);
        }

        public override void Execute(WorkContext context)
        {
            if (logger.IsDebugEnabled)
            {
                logger.Debug("Going to touch the following documents (missing references, need to check for concurrent transactions): {0}",
                    string.Join(", ", Keys));
            }
            var set = context.DoNotTouchAgainIfMissingReferences.GetOrAdd(Index, _ => new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase));
            context.TransactionalStorage.Batch(accessor =>
            {
                foreach (var key in Keys)
                {
                    set.Add(key);
                    try
                    {
                        Etag preTouchEtag;
                        Etag afterTouchEtag;
                        accessor.Documents.TouchDocument(key, out preTouchEtag, out afterTouchEtag);
                    }
                    catch (ConcurrencyException)
                    {
                    }
                }
            });
        }

        public override Task Clone()
        {
            return new TouchMissingReferenceDocumentTask
            {
                Index = Index,
                Keys = new HashSet<string>(Keys)
            };
        }
    }
}
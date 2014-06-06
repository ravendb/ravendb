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
	public class TouchReferenceDocumentIfChangedTask : Task
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();
		public IDictionary<string, Etag> ReferencesToCheck { get; set; }

		public override string ToString()
		{
			return string.Format("Index: {0}, References: {1}", Index, string.Join(", ", ReferencesToCheck.Keys));
		}


		public override bool SeparateTasksByIndex
		{
			get { return false; }
		}

		public override void Merge(Task task)
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

			var docsToTouch = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			context.TransactionalStorage.Batch(accessor =>
			{
				foreach (var kvp in ReferencesToCheck)
				{
					foreach (var index in context.IndexStorage.Indexes)
					{
						var set = context.DoNotTouchAgainIfCheckingReferences.GetOrAdd(index,
							_ => new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase));
						set.Add(kvp.Key);
					}

					var doc = accessor.Documents.DocumentMetadataByKey(kvp.Key, null);

					if (doc == null || doc.Etag == kvp.Value)
						continue;


					docsToTouch.Add(kvp.Key);
				}
			});

			using (context.Database.DocumentLock.Lock())
			{
				context.TransactionalStorage.Batch(accessor =>
				{
					foreach (var doc in docsToTouch)
					{
						try
						{
							Etag preTouchEtag;
							Etag afterTouchEtag;
							accessor.Documents.TouchDocument(doc, out preTouchEtag, out afterTouchEtag);
						}
						catch (ConcurrencyException)
						{
						}
						context.Database.CheckReferenceBecauseOfDocumentUpdate(doc, accessor);
					}
				});
			}

		}

		public override Task Clone()
		{
			return new TouchReferenceDocumentIfChangedTask
			{
				Index = Index,
				ReferencesToCheck = new Dictionary<string, Etag>(ReferencesToCheck, StringComparer.OrdinalIgnoreCase)
			};
		}
	}
}

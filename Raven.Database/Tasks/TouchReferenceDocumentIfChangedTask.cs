using System;
using System.Collections.Generic;
using System.Linq;
using Mono.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Database.Indexing;
using Raven.Database.Util;

namespace Raven.Database.Tasks
{
	public class TouchReferenceDocumentIfChangedTask : DatabaseTask
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
							logger.Debug("Cannot touch {0}, non existent document", kvp.Key);
							continue;
						}
						if (doc.Etag == kvp.Value)
						{
							logger.Debug("Don't need to touch {0}, etag {1} is the same as when we last saw it", kvp.Key, doc.Etag);
							continue;
						}

						docsToTouch.Add(kvp.Key);

						var entityName = doc.Metadata.Value<string>(Constants.RavenEntityName);

						if(string.IsNullOrEmpty(entityName))
							continue;

						Etag highestEtagInCollection;

						if (collectionsAndEtags.TryGetValue(entityName, out highestEtagInCollection) == false || doc.Etag.CompareTo(highestEtagInCollection) > 0)
						{
							collectionsAndEtags[entityName] = doc.Etag;
						}
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
								logger.Info("Concurrency exception when touching {0}", doc);
							}
							context.Database.Indexes.CheckReferenceBecauseOfDocumentUpdate(doc, accessor);
						}

						foreach (var collectionEtagPair in collectionsAndEtags)
						{
							context.Database.LastCollectionEtags.Update(collectionEtagPair.Key, collectionEtagPair.Value);
						}
					});
				}

			}
		}

		public override DatabaseTask Clone()
		{
			return new TouchReferenceDocumentIfChangedTask
			{
				Index = Index,
				ReferencesToCheck = new Dictionary<string, Etag>(ReferencesToCheck, StringComparer.OrdinalIgnoreCase)
			};
		}
	}
}
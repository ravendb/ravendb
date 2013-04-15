// -----------------------------------------------------------------------
//  <copyright file="DatabaseEtagSynchronizer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Indexing;

namespace Raven.Database.Impl
{
	public class DatabaseEtagSynchronizer
	{
		private readonly ReaderWriterLockSlim slim = new ReaderWriterLockSlim();

		private readonly WorkContext context;

		private BatchEdgeEtags lastBatchEdgeEtags;
		public BatchEdgeEtags LastBatchEdgeEtags
		{
			get { return GetLastBatchEdgeEtags(); }
		}

		public DatabaseEtagSynchronizer(WorkContext context, Etag lastSeenEtag)
		{
			this.context = context;
			lastBatchEdgeEtags = new BatchEdgeEtags(lastSeenEtag, lastSeenEtag);
		}

		public void UpdateSynchronizationState(JsonDocument[] docs)
		{
			if (docs == null)
				return;

			var edgeEtags = GetEdgeEtags(docs);
			var currentLastBatchEdgeEtags = GetLastBatchEdgeEtags();

			if (context.RunIndexing)
			{
				
			}
		}

		private BatchEdgeEtags GetLastBatchEdgeEtags()
		{
			try
			{
				slim.EnterReadLock();
				return lastBatchEdgeEtags;
			}
			finally
			{
				slim.ExitReadLock();
			}
		}

		private static BatchEdgeEtags GetEdgeEtags(IList<JsonDocument> documents)
		{
			if (documents == null || documents.Count == 0)
				return new BatchEdgeEtags(Etag.Empty, Etag.Empty);

			var minEtag = documents[0].Etag;
			var maxEtag = documents[0].Etag;

			for (var i = 1; i < documents.Count; i++)
			{
				var etag = documents[i].Etag;

				if (minEtag.CompareTo(etag) > 0)
				{
					minEtag = etag;
				}

				if (maxEtag.CompareTo(etag) < 0)
				{
					maxEtag = etag;
				}
			}

			return new BatchEdgeEtags(minEtag, maxEtag);
		}

		public class BatchEdgeEtags
		{
			public BatchEdgeEtags(Etag minEtag, Etag maxEtag)
			{
				MinEtag = minEtag;
				MaxEtag = maxEtag;
			}

			public Etag MinEtag { get; private set; }

			public Etag MaxEtag { get; private set; }
		}
	}
}
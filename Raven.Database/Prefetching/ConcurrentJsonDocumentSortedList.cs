// -----------------------------------------------------------------------
//  <copyright file="ConcurrentOrderedList.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;

namespace Raven.Database.Prefetching
{
	public class ConcurrentJsonDocumentSortedList
	{
		private readonly ReaderWriterLockSlim slim = new ReaderWriterLockSlim();

		private readonly IList<JsonDocument> innerList;

		public ConcurrentJsonDocumentSortedList()
		{
			innerList = new List<JsonDocument>();
		}

		public int Count
		{
			get
			{
				try
				{
					slim.EnterReadLock();
					return innerList.Count;
				}
				finally
				{
					slim.ExitReadLock();
				}
			}
		}

		public void Add(JsonDocument value)
		{
			try
			{
				slim.EnterWriteLock();
				var index = CalculateEtagIndex(value.Etag);
				innerList.Insert(index, value);
			}
			finally
			{
				slim.ExitWriteLock();
			}
		}

		public bool TryPeek(out JsonDocument result)
		{
			try
			{
				slim.EnterReadLock();
				result = innerList.FirstOrDefault();

				return result != null;
			}
			finally
			{
				slim.ExitReadLock();
			}
		}

		public bool TryDequeue(out JsonDocument result)
		{
			try
			{
				slim.EnterWriteLock();
				result = innerList.FirstOrDefault();
				if (result != null)
					innerList.RemoveAt(0);

				return result != null;
			}
			finally
			{
				slim.ExitWriteLock();
			}
		}

		public Etag NextDocumentETag()
		{
			JsonDocument result;
			return TryPeek(out result) == false ? null : result.Etag;
		}

		private int CalculateEtagIndex(Etag etag)
		{
			int i;
			for (i = innerList.Count; i > 0; i--)
			{
				var elementEtag = innerList[i - 1].Etag;
				if (elementEtag.CompareTo(etag) < 0)
					return i;
			}

			return i;
		}

		public Etag GetFirstETagGap()
		{
			slim.EnterReadLock();

			try
			{
				if (innerList.Count == 0)
				{
					return null;
				}

				// look for the first gap of etag
				for (var i = 0; i < innerList.Count - 1; i++)
				{
					var oneUp = innerList[i].Etag.IncrementBy(1);
					if (oneUp.Equals(innerList[i + 1].Etag) == false)
					{
						return oneUp;
					}
				}

				return innerList[innerList.Count - 1].Etag; // take the last one
			}
			finally
			{
				slim.ExitReadLock();
			}
		}
	}
}
// -----------------------------------------------------------------------
//  <copyright file="ConcurrentOrderedList.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
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

	    private int loadedSize;

		public ConcurrentJsonDocumentSortedList()
		{
			innerList = new List<JsonDocument>();
		}

		public IEnumerable<JsonDocument> Clone()
		{
			try
			{
				slim.EnterReadLock();
				return new List<JsonDocument>(innerList);
			}
			finally
			{
				slim.ExitReadLock();
			}
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
			    loadedSize += value.SerializedSizeOnDisk;
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
			    {
					innerList.RemoveAt(0);
			        loadedSize -= result.SerializedSizeOnDisk;
			    }

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

	    public int LoadedSize
	    {
	        get
		{
			slim.EnterReadLock();
			try
			{
                    return loadedSize;
	            }
	            finally
				{
	                slim.ExitReadLock();
				}
	        }
	    }

	    public T Aggregate<T>(T seed, Func<T, JsonDocument, T> aggregate)
				{
            slim.EnterReadLock();
	        try
					{
	            return innerList.Aggregate(seed, aggregate);
					}
			finally
			{
				slim.ExitReadLock();
			}
		}

		public T Aggregate<T>(T seed, Func<T, JsonDocument, T> aggregate)
		{
			slim.EnterReadLock();
			try
			{
				return innerList.Aggregate(seed, aggregate);
	}
			finally
			{
				slim.ExitReadLock();
}
		}
	}
}
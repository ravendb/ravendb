using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Channels;
using Microsoft.Isam.Esent.Interop;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public static class IndexReaderBuffers
	{
		private static BufferManager buffers = BufferManager.CreateBufferManager(1024 * 1024 * 256, SystemParameters.BookmarkMost);

		public static BufferManager Buffers
		{
			get { return buffers; }
		}
	}

	// optimized according to this: http://managedesent.codeplex.com/discussions/274843#post680337
	public class OptimizedIndexReader<T>
		where T : class
	{
		private readonly List<Key> primaryKeyIndexes;
		
		private class Key
		{
			public byte[] Buffer;
			public T State;
			public int BufferLen;
			public int Index;
		}

		private Func<T, bool> filter;

		public OptimizedIndexReader(int size)
		{
			primaryKeyIndexes = new List<Key>(size);
			
		}

		public int Count
		{
			get { return primaryKeyIndexes.Count; }
		}


		public bool Add(JET_SESID session, JET_TABLEID table, T item)
		{
			byte[] buffer;
			int actualBookmarkSize;

			var largeBuffer = IndexReaderBuffers.Buffers.TakeBuffer(SystemParameters.BookmarkMost);
			try
			{
				Api.JetGetBookmark(session, table, largeBuffer,
								   largeBuffer.Length, out actualBookmarkSize);

				buffer = IndexReaderBuffers.Buffers.TakeBuffer(actualBookmarkSize);
				Buffer.BlockCopy(largeBuffer, 0, buffer, 0, actualBookmarkSize);
			}
			finally
			{
				IndexReaderBuffers.Buffers.ReturnBuffer(largeBuffer);
			}
			if (primaryKeyIndexes.Any(x =>
			{
				if (x.BufferLen != actualBookmarkSize)
					return false;
				for (int i = 0; i < actualBookmarkSize; i++)
				{
					if (buffer[i] != x.Buffer[i])
						return false;
				}
				return true;
			}))
				return false;
			primaryKeyIndexes.Add(new Key
			{
				Buffer = buffer,
				BufferLen = actualBookmarkSize,
				Index = primaryKeyIndexes.Count,
				State = item
			});
			return true;
		}

		public IEnumerable<Tuple<byte[], int>> GetSortedBookmarks()
		{
			SortPrimaryKeys();
			foreach (var key in primaryKeyIndexes)
			{
				yield return Tuple.Create(key.Buffer, key.BufferLen);
				IndexReaderBuffers.Buffers.ReturnBuffer(key.Buffer);
			}
		}

		public IEnumerable<TResult> Select<TResult>(JET_SESID session, JET_TABLEID table, Func<T, TResult> func)
		{
			SortPrimaryKeys();

			return primaryKeyIndexes.Select(key =>
			{
				Api.JetGotoBookmark(session, table, key.Buffer, key.BufferLen);
				IndexReaderBuffers.Buffers.ReturnBuffer(key.Buffer);

				if (filter != null && filter(key.State) == false)
					return null;

				var result = new {Result = func(key.State), key.Index};


				return result;
			})
				.OrderBy(x => x.Index)
				.Select(x => x.Result);
		}

		private void SortPrimaryKeys()
		{
			primaryKeyIndexes.Sort((x, y) =>
			{
				for (int i = 0; i < Math.Min(x.BufferLen, y.BufferLen); i++)
				{
					if (x.Buffer[i] != y.Buffer[i])
						return x.Buffer[i] - y.Buffer[i];
				}
				return x.BufferLen - y.BufferLen;
			});
		}

		public OptimizedIndexReader<T> Where(Func<T, bool> predicate)
		{
			filter = predicate;
			return this;
		}
	}

	public class OptimizedIndexReader : OptimizedIndexReader<object>
	{
		public OptimizedIndexReader() : base(32)
		{
			
		}

		public bool Add(JET_SESID session, JET_TABLEID table)
		{
			return Add(session, table, null);
		}

		public OptimizedIndexReader Where(Func<bool> filter)
		{
			return (OptimizedIndexReader)Where(_ => filter());
		} 

		public IEnumerable<TResult> Select<TResult>(JET_SESID session, JET_TABLEID table, Func<TResult> func)
		{
			return Select(session, table, _ => func());
		}
	}
}

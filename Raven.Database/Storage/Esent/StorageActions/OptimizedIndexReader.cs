using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Channels;
using Microsoft.Isam.Esent.Interop;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
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

		private static BufferManager buffers = BufferManager.CreateBufferManager(1024*1024*256, SystemParameters.BookmarkMost);

		[ThreadStatic]
		private static byte[] bookmarkBuffer;
		private readonly JET_SESID session;
		private readonly JET_TABLEID table;
		private Func<T, bool> filter;

		public OptimizedIndexReader(JET_SESID session, JET_TABLEID table, int size)
		{
			primaryKeyIndexes = new List<Key>(size);
			this.table = table;
			this.session = session;
			if (bookmarkBuffer == null)
			{
				bookmarkBuffer = new byte[SystemParameters.BookmarkMost];
			}
		}

		public int Count
		{
			get { return primaryKeyIndexes.Count; }
		}


		public void Add(T item)
		{
			int actualBookmarkSize;
			Api.JetGetBookmark(session, table, bookmarkBuffer,
			                   bookmarkBuffer.Length, out actualBookmarkSize);

			var buffer = buffers.TakeBuffer(actualBookmarkSize);
			Buffer.BlockCopy(bookmarkBuffer, 0, buffer, 0, actualBookmarkSize);
			primaryKeyIndexes.Add(new Key
			{
				Buffer = buffer,
				BufferLen = actualBookmarkSize,
				Index = primaryKeyIndexes.Count,
				State = item
			});
		}

		public IEnumerable<Tuple<byte[], int>> GetSortedBookmarks()
		{
			SortPrimaryKeys();
			foreach (var key in primaryKeyIndexes)
			{
				yield return Tuple.Create(key.Buffer, key.BufferLen);
				buffers.ReturnBuffer(key.Buffer);
			}
		}

		public IEnumerable<TResult> Select<TResult>(Func<T,TResult> func)
		{
			SortPrimaryKeys();

			return primaryKeyIndexes.Select(key =>
			{
				var bookmark = key.Buffer;
				Api.JetGotoBookmark(session, table, bookmark, bookmark.Length);
				buffers.ReturnBuffer(key.Buffer);

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
		public OptimizedIndexReader(JET_SESID session, JET_TABLEID table, int size) : base(session, table, size)
		{
		}

		public void Add()
		{
			Add(null);
		}

		public OptimizedIndexReader Where(Func<bool> filter)
		{
			return (OptimizedIndexReader)Where(_ => filter());
		} 

		public IEnumerable<TResult> Select<TResult>(Func<TResult> func)
		{
			return Select(_ => func());
		}
	}
}

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	// optimized according to this: http://managedesent.codeplex.com/discussions/274843#post680337
	public class OptimizedIndexReader<T>
	{
		private readonly List<Tuple<byte[], T>> primaryKeyIndexes = new List<Tuple<byte[], T>>();
		public readonly Dictionary<T, int> originalPos = new Dictionary<T, int>();

		private readonly byte[] bookmarkBuffer;
		private readonly byte[] ignoredBuffer;
		private readonly JET_SESID session;
		private readonly JET_TABLEID table;
		private Func<T, bool> filter;

		public OptimizedIndexReader(JET_SESID session, JET_TABLEID table)
		{
			this.table = table;
			this.session = session;
			bookmarkBuffer = new byte[SystemParameters.BookmarkMost];
			ignoredBuffer = new byte[SystemParameters.BookmarkMost];
		}


		public void Add(T item)
		{
			int actualBookmarkSize;
			int ignored;
			Api.JetGetSecondaryIndexBookmark(session, table, ignoredBuffer, ignoredBuffer.Length, out ignored, bookmarkBuffer,
											 bookmarkBuffer.Length, out actualBookmarkSize, GetSecondaryIndexBookmarkGrbit.None);

			originalPos.Add(item,primaryKeyIndexes.Count);
			primaryKeyIndexes.Add(Tuple.Create(bookmarkBuffer.Take(actualBookmarkSize).ToArray(), item));
		}

		public void Get()
		{
			primaryKeyIndexes.Sort((x, y) =>
			{
				var bytes1 = x.Item1;
				var bytes2 = y.Item1;
				for (int i = 0; i < Math.Min(bytes1.Length, bytes2.Length); i++)
				{
					if (bytes1[i] != bytes2[i])
						return bytes1[i] - bytes2[i];
				}
				return bytes1.Length - bytes2.Length;
			});
		}

		public IEnumerable<TResult> Select<TResult>(Func<T,TResult> func)
		{
			var results = new List<Tuple<TResult, T>>();
			foreach (var primaryKeyIndexTuple in primaryKeyIndexes)
			{
				var bookmark = primaryKeyIndexTuple.Item1;
				var item = primaryKeyIndexTuple.Item2;
				Api.JetGotoBookmark(session, table, bookmark, bookmark.Length);

				if(filter!=null && filter(item) == false)
					continue;


				results.Add(Tuple.Create(func(item), item));
			}

			return results.OrderBy(x => originalPos[x.Item2]).Select(x => x.Item1);
		}

		public OptimizedIndexReader<T> Where(Func<T, bool> predicate)
		{
			this.filter = predicate;
			return this;
		}
	}
}
// -----------------------------------------------------------------------
//  <copyright file="ConcurrentJsonDocumentSortedList.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Prefetching;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Synchronization
{
	public class ConcurrentJsonDocumentSortedListTests : NoDisposalNeeded
	{
		[Fact]
		public void ConcurrentJsonDocumentSortedListShouldSortByEtag()
		{
			var list = new ConcurrentJsonDocumentSortedList();

			var etag1 = EtagUtil.Increment(Etag.Empty, 1);
			var etag2 = EtagUtil.Increment(Etag.Empty, 2);
			var etag3 = EtagUtil.Increment(Etag.Empty, 3);
			var etag4 = EtagUtil.Increment(Etag.Empty, 4);

			var doc1 = new JsonDocument
			{
				Etag = etag1
			};

			var doc2 = new JsonDocument
			{
				Etag = etag2
			};

			var doc3 = new JsonDocument
			{
				Etag = etag3
			};

			var doc4 = new JsonDocument
			{
				Etag = etag4
			};

			list.Add(doc4);
			list.Add(doc2);
			list.Add(doc1);
			list.Add(doc3);

			JsonDocument result;

			Assert.True(list.TryDequeue(out result));
			Assert.Equal(doc1.Etag, result.Etag);

			Assert.True(list.TryDequeue(out result));
			Assert.Equal(doc2.Etag, result.Etag);

			Assert.True(list.TryDequeue(out result));
			Assert.Equal(doc3.Etag, result.Etag);

			Assert.True(list.TryDequeue(out result));
			Assert.Equal(doc4.Etag, result.Etag);
		}
	}
}
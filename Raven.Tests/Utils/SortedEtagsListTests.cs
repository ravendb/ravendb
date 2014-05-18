// -----------------------------------------------------------------------
//  <copyright file="SortedEtagsListTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Util;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Utils
{
	public class SortedEtagsListTests : NoDisposalNeeded
	{
		private Etag zero = Etag.Empty;

		private Etag ToEtag(int value)
		{
			return EtagUtil.Increment(zero, value);
		}

		[Fact]
		public void EmptyList()
		{
			var list = new SortedKeyList<Etag>();

			list.RemoveSmallerOrEqual(zero);

			Assert.Equal(0, list.Count);
		}

		[Fact]
		public void CanAddDuplicate()
		{
			var list = new SortedKeyList<Etag>();

			list.Add(ToEtag(1));
			list.Add(ToEtag(1));

			Assert.Equal(1, list.Count);
		}

		[Fact]
		public void ShouldNotRemoveAnything()
		{
			var list = new SortedKeyList<Etag>();

			list.Add(ToEtag(6));
			list.Add(ToEtag(8));
			list.Add(ToEtag(7));

			list.RemoveSmallerOrEqual(ToEtag(5));

			Assert.Equal(3, list.Count);
		}

		[Fact]
		public void ShouldRemainOneItem_RemovingItemIsntOnTheList()
		{
			var list = new SortedKeyList<Etag>();

			list.Add(ToEtag(1));
			list.Add(ToEtag(3));
			list.Add(ToEtag(5));

			list.RemoveSmallerOrEqual(ToEtag(4));

			Assert.Equal(1, list.Count);
			Assert.True(list.Contains(ToEtag(5)));
		}

		[Fact]
		public void ShouldRemainTwoItems_RemovingItemIsntOnTheList()
		{
			var list = new SortedKeyList<Etag>();

			list.Add(ToEtag(1));
			list.Add(ToEtag(2));
			list.Add(ToEtag(4));
			list.Add(ToEtag(5));

			list.RemoveSmallerOrEqual(ToEtag(3));

			Assert.Equal(2, list.Count);
			Assert.True(list.Contains(ToEtag(4)));
			Assert.True(list.Contains(ToEtag(5)));
		}

		[Fact]
		public void ShouldRemainOneItem_RemovingItemIsOnTheList()
		{
			var list = new SortedKeyList<Etag>();

			list.Add(ToEtag(1));
			list.Add(ToEtag(3));
			list.Add(ToEtag(5));

			list.RemoveSmallerOrEqual(ToEtag(3));

			Assert.Equal(1, list.Count);
			Assert.True(list.Contains(ToEtag(5)));
		}

		[Fact]
		public void ShouldRemainTwoItems_RemovingItemIsOnTheList()
		{
			var list = new SortedKeyList<Etag>();

			list.Add(ToEtag(1));
			list.Add(ToEtag(2));
			list.Add(ToEtag(4));
			list.Add(ToEtag(5));

			list.RemoveSmallerOrEqual(ToEtag(2));

			Assert.Equal(2, list.Count);
			Assert.True(list.Contains(ToEtag(4)));
			Assert.True(list.Contains(ToEtag(5)));
		}
	}
}
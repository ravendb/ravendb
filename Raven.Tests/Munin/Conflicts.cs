//-----------------------------------------------------------------------
// <copyright file="Conflicts.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
{
	public class Conflicts : SimpleFileTest
	{
		[Fact]
		public void TwoTxCannotAddSameDataBeforeCommit()
		{
			Assert.True(Table.Put(RavenJToken.FromObject("a"), new byte[] { 1 }));

			SuppressTx(() => Assert.False(Table.Put(RavenJToken.FromObject("a"), new byte[] { 1 })));
		}

		[Fact]
		public void OneTxCannotDeleteTxThatAnotherTxAddedBeforeCommit()
		{
			Assert.True(Table.Put(RavenJToken.FromObject("a"), new byte[] { 1 }));

			SuppressTx(() => Assert.False(Table.Remove(RavenJToken.FromObject("a"))));
		}


		[Fact]
		public void TwoTxCanAddSameDataAfterCommit()
		{
			Assert.True(Table.Put(RavenJToken.FromObject("a"), new byte[] { 1 }));

			Commit();

			Assert.True(Table.Put(RavenJToken.FromObject("a"), new byte[] { 1 }));
		}

		[Fact]
		public void OneTxCanDeleteTxThatAnotherTxAddedAfterCommit()
		{

			Assert.True(Table.Put(RavenJToken.FromObject("a"), new byte[] { 1 }));

			Commit();

			Assert.True(Table.Remove(RavenJToken.FromObject("a")));
		}
	}
}
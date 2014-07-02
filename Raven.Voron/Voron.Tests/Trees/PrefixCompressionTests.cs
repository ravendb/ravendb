// -----------------------------------------------------------------------
//  <copyright file="PrefixCompressionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Voron.Tests.Trees
{
	public class PrefixCompressionTests : StorageTest
	{
		[Fact]
		public void BasicCheck()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "prefixed-tree");

				tree.Add("users/11", StreamFor("abc"));
				tree.Add("users/12", StreamFor("def"));
				tree.Add("users/20", StreamFor("ghi"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("prefixed-tree");

				Assert.Equal("abc", tree.Read("users/11").Reader.AsSlice());
				Assert.Equal("def", tree.Read("users/12").Reader.AsSlice());
				Assert.Equal("ghi", tree.Read("users/20").Reader.AsSlice());

				tx.Commit();
			}
		}
	}
}
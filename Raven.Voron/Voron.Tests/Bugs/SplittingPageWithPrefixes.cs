// -----------------------------------------------------------------------
//  <copyright file="SplittingPageWithPrefixes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class SplittingPageWithPrefixes : StorageTest
	{
		[Fact]
		public void ShouldHaveEnoughSpaceOnNewPageDuringTruncate()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "prefixed-tree", keysPrefixing: true);

				tx.Commit();
			}

			var r = new Random(1);

			for (int i = 0; i < 1000; i++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = tx.ReadTree("prefixed-tree");

					tree.Add(new string('a', r.Next(2000)) + i, new MemoryStream(new byte[128]));
					tree.Add(new string('b', r.Next(1000)) + i, new MemoryStream(new byte[256]));
					tree.Add(new string('c', r.Next(2000)) + i, new MemoryStream(new byte[128]));
					tree.Add(new string('d', r.Next(500)) + i, new MemoryStream(new byte[512]));

					tx.Commit();
				}
			}
		}

		[Fact]
		public void ShouldHaveEnoughSpaceOnNewPageWhenSplittingPageInHalf()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "prefixed-tree", keysPrefixing: true);

				tx.Commit();
			}

			var r = new Random(3);

			for (int i = 0; i < 1000; i++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = tx.ReadTree("prefixed-tree");

					tree.Add(new string('a', r.Next(2000)) + i, new MemoryStream(new byte[128]));
					tree.Add(new string('b', r.Next(1000)) + i, new MemoryStream(new byte[256]));
					tree.Add(new string('c', r.Next(2000)) + i, new MemoryStream(new byte[128]));
					tree.Add(new string('d', r.Next(500)) + i, new MemoryStream(new byte[512]));

					tx.Commit();
				}
			}
		}
	}
}
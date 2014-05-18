// -----------------------------------------------------------------------
//  <copyright file="EdgeCases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Journal
{
	public class EdgeCases : StorageTest
	{
		// all tests here relay on the fact than one log file can contains max 10 pages
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * AbstractPager.PageSize;
		}

		[Fact]
		public void TransactionCommitShouldSetCurrentLogFileToNullIfItIsFull()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[4 * AbstractPager.PageSize]; 
				tx.State.Root.Add(tx, "items/0", new MemoryStream(bytes));
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[1 * AbstractPager.PageSize];
				tx.State.Root.Add(tx, "items/1", new MemoryStream(bytes));
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[1 * AbstractPager.PageSize];
				tx.State.Root.Add(tx, "items/1", new MemoryStream(bytes));
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[1 * AbstractPager.PageSize];
				tx.State.Root.Add(tx, "items/1", new MemoryStream(bytes));
				tx.Commit();
			}

			Assert.Null(Env.Journal.CurrentFile);
			Assert.Equal(1, Env.Journal.Files.Count);
		}
	}
}
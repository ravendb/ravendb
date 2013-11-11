// -----------------------------------------------------------------------
//  <copyright file="EdgeCases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;

namespace Voron.Tests.Log
{
	public class EdgeCases : StorageTest
	{
		// all tests here relay on the fact than one log file can contains max 10 pages
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * options.DataPager.PageSize;
		}

		[Fact]
		public void TransactionCommitShouldSetCurrentLogFileToNullIfItIsFull()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[4 * Env.PageSize]; 
				tx.State.Root.Add(tx, "items/0", new MemoryStream(bytes));
				tx.Commit();
			}

			Assert.Null(Env.Journal.CurrentFile);
			Assert.Equal(1, Env.Journal.Files.Count);
		}
	}
}
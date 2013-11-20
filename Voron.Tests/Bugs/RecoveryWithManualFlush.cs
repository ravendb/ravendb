// -----------------------------------------------------------------------
//  <copyright file="RecoveryWithManualFlush.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class RecoveryWithManualFlush : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.ManualFlushing = true;
		}

		[Fact]
		public void StorageRecoveryAfterFlushingToDataFile()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "items/1", new MemoryStream(new byte[] { 1, 2, 3 }));
				tx.Commit();
			}

			Env.FlushLogToDataFile();

			RestartDatabase();

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var readResult = tx.State.Root.Read(tx, "items/1");

				Assert.NotNull(readResult);
				Assert.Equal(3, readResult.Stream.Length);
			}
		} 
	}
}
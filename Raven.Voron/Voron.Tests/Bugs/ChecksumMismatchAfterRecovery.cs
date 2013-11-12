// -----------------------------------------------------------------------
//  <copyright file="ChecksumMismatchAfterRecovery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class ChecksumMismatchAfterRecovery : IDisposable
	{
		private const string _dataPath = "test-checksum-mismatch.data";

		public ChecksumMismatchAfterRecovery()
		{
			DeleteDir();
		}

		private void DeleteDir()
		{
			if (Directory.Exists(_dataPath))
				Directory.Delete(_dataPath, true);
		}

		[Fact]
		public void ShouldNotThrowChecksumMismatch()
		{
			var random = new Random(1);
			var buffer = new byte[100];
			random.NextBytes(buffer);

			for (int i = 0; i < 100; i++)
			{
				buffer[i] = 13;
			}

			var options = StorageEnvironmentOptions.ForPath(_dataPath);

			using (var env = new StorageEnvironment(options))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (int i = 0; i < 50; i++)
					{
						tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
					}

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (int i = 50; i < 100; i++)
					{
						tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
					}

					tx.Commit();
				}
			}

			options = StorageEnvironmentOptions.ForPath(_dataPath);

			using (var env = new StorageEnvironment(options))
			{
				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{

					for (int i = 0; i < 100; i++)
					{
						var readResult = tx.State.Root.Read(tx, "items/" + i);
						Assert.NotNull(readResult);
						var memoryStream = new MemoryStream();
						readResult.Stream.CopyTo(memoryStream);
						Assert.Equal(memoryStream.ToArray(), buffer);
					}
				}
			}
		}

		public void Dispose()
		{
			DeleteDir();
		}
	}
}
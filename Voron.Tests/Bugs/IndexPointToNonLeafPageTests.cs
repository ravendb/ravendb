// -----------------------------------------------------------------------
//  <copyright file="IndexPointToNotLeafPageTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class IndexPointToNonLeafPageTests : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.ManualFlushing = true;
		}

		[Fact]
		public void ShouldProperlyMovePositionForNextPageAllocationInScratchBufferPool()
		{
			var sequentialLargeIds = ReadData("non-leaf-page-seq-id-large-values.txt");

			var enumerator = sequentialLargeIds.GetEnumerator();

			for (var transactions = 0; transactions < 36; transactions++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 100; i++)
					{
						enumerator.MoveNext();

						tx.State.Root.Add(tx, enumerator.Current.Key.ToString("0000000000000000"), new MemoryStream(enumerator.Current.Value));
					}

					tx.Commit();
				}

				Env.FlushLogToDataFile();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				foreach (var item in sequentialLargeIds)
				{
					var readResult = tx.State.Root.Read(tx, item.Key.ToString("0000000000000000"));

					Assert.NotNull(readResult);

					Assert.Equal(item.Value.Length, readResult.Reader.Length);
				}
			}
		}

		private IDictionary<long, byte[]> ReadData(string fileName)
		{
			using (var reader = new StreamReader("Bugs/Data/" + fileName))
			{
				string line;

				var random = new Random();
				var results = new Dictionary<long, byte[]>();

				while (!string.IsNullOrEmpty(line = reader.ReadLine()))
				{
					var l = line.Trim().Split(':');

					var buffer = new byte[int.Parse(l[1])];
					random.NextBytes(buffer);

					results.Add(long.Parse(l[0]), buffer);
				}

				return results;
			}
		}
	}
}
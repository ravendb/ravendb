// -----------------------------------------------------------------------
//  <copyright file="VoronTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Voron;
using Voron.Impl;

namespace Performance.Comparison.Voron
{
	public class VoronTest : IStoragePerformanceTest
	{
		private readonly FlushMode flushMode;
		private const string dataDir = "voron-perf-test";
		private readonly string dataPath;

		public VoronTest(string path, FlushMode flushMode)
		{
			this.flushMode = flushMode;
			dataPath = Path.Combine(path, dataDir);
		}

		public string StorageName { get { return "Voron"; } }

		private void NewStorage()
		{
			if (Directory.Exists(dataPath))
				Directory.Delete(dataPath, true); //TODO do it in more robust way
		}

		public List<PerformanceRecord> WriteSequential()
		{
			var sequentialIds = Enumerable.Range(0, Constants.ItemsPerTransaction * Constants.WriteTransactions);

			return Write(string.Format("[Voron] sequential write ({0} items)", Constants.ItemsPerTransaction), sequentialIds,
						 Constants.ItemsPerTransaction, Constants.WriteTransactions);
		}


		public List<PerformanceRecord> WriteRandom(HashSet<int> randomIds)
		{
			return Write(string.Format("[Voron] random write ({0} items)", Constants.ItemsPerTransaction), randomIds,
						 Constants.ItemsPerTransaction, Constants.WriteTransactions);
		}

		public PerformanceRecord ReadSequential()
		{
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

			return Read(string.Format("[Voron] sequential read ({0} items)", Constants.ReadItems), sequentialIds);
		}

		public PerformanceRecord ReadRandom(HashSet<int> randomIds)
		{
			return Read(string.Format("[Voron] random read ({0} items)", Constants.ReadItems), randomIds);
		}

		private List<PerformanceRecord> Write(string operation, IEnumerable<int> ids, int itemsPerTransaction, int numberOfTransactions)
		{
			NewStorage();

			var records = new List<PerformanceRecord>();

			var value = new byte[128];
			new Random().NextBytes(value);
			 var ms = new MemoryStream(value);

			var sw = new Stopwatch();

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(dataPath, flushMode)))
            {
				var enumerator = ids.GetEnumerator();
				sw.Restart();
				for (var transactions = 0; transactions < numberOfTransactions; transactions++)
				{
					sw.Restart();
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						for (var i = 0; i < itemsPerTransaction; i++)
						{
							enumerator.MoveNext();
							ms.Position = 0;
							tx.State.Root.Add(tx, enumerator.Current.ToString("0000000000000000"), ms);
						}

						tx.Commit();
					}
					sw.Stop();

					records.Add(new PerformanceRecord
					{
						Operation = operation,
						Time = DateTime.Now,
						Duration = sw.ElapsedMilliseconds,
						ProcessedItems = itemsPerTransaction
					});
				}

				sw.Stop();
			}

			return records;
		}

		private PerformanceRecord Read(string operation, IEnumerable<int> ids)
		{
			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(dataPath)))
			{
				var ms = new byte[128];
				var sw = Stopwatch.StartNew();

				var processed = 0;

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					foreach (var id in ids)
					{

						var key = id.ToString("0000000000000000");
						using (var stream = tx.State.Root.Read(tx, key).Stream)
						{
							while (stream.Read(ms, 0, ms.Length) != 0)
							{
							}
						}

						processed++;
					}
				}

				sw.Stop();

				return new PerformanceRecord
				{
					Operation = operation,
					Time = DateTime.Now,
					Duration = sw.ElapsedMilliseconds,
					ProcessedItems = processed
				};
			}
		}
	}
}
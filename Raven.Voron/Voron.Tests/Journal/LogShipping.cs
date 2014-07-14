using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;
using Xunit;

namespace Voron.Tests.Journal
{
	public unsafe class LogShipping : StorageTest
	{
		public LogShipping()
			: base(StorageEnvironmentOptions.CreateMemoryOnly())
		{
		}

		[Fact]
		public void Committing_tx_should_fire_event_with_transactionsToShip_records()
		{
			var transactionsToShip = new ConcurrentQueue<TransactionToShip>();
			Env.Journal.OnTransactionCommit += tx =>
			{
				tx.CreatePagesSnapshot();
				transactionsToShip.Enqueue(tx);
			};

			WriteTestDataToEnv();

			Assert.Equal(3, transactionsToShip.Count);

			uint previousCrc = 0;
			//validate crc
			foreach (var tx in transactionsToShip)
			{
				fixed (byte* pageDataBufferPtr = tx.PagesSnapshot)
				{
					//calculate crc, but skip the first page --> it is a header
					var crc = Crc.Value(pageDataBufferPtr + AbstractPager.PageSize, 0, tx.PagesSnapshot.Length - AbstractPager.PageSize);
					Assert.Equal(tx.Header.Crc, crc);

					if (previousCrc != 0)
						Assert.Equal(previousCrc, tx.PreviousTransactionCrc);
					previousCrc = tx.Header.Crc;
				}
			}
		}

		[Fact]
		public void StorageEnvironment_Two_Different_Tx_Should_be_shipped_properly1()
		{
			var transactionsToShip = new ConcurrentQueue<TransactionToShip>();
			Env.Journal.OnTransactionCommit += tx =>
			{
				tx.CreatePagesSnapshot();
				transactionsToShip.Enqueue(tx);
			};

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "TestTree");
				tree.Add("ABC", "Foo");
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "TestTree2");
				tree.Add("ABC", "Foo");
				tx.Commit();
			}

			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				TransactionToShip tx;
				transactionsToShip.TryDequeue(out tx);
				shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot, tx.PreviousTransactionCrc);

				using (var snaphsot = shippingDestinationEnv.CreateSnapshot())
				{
					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo", result.Reader.ToStringValue());
					});
				}

				transactionsToShip.TryDequeue(out tx);
				shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot, tx.PreviousTransactionCrc);

				using (var snaphsot = shippingDestinationEnv.CreateSnapshot())
				{
					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo", result.Reader.ToStringValue());
					});

					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree2", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo", result.Reader.ToStringValue());
					});
				}
			}
		}


		[Fact]
		public void StorageEnvironment_Two_Different_Tx_Should_be_shipped_properly2()
		{
			var transactionsToShip = new ConcurrentQueue<TransactionToShip>();
			Env.Journal.OnTransactionCommit += tx =>
			{
				tx.CreatePagesSnapshot();
				transactionsToShip.Enqueue(tx);
			};

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "TestTree");
				tree.Add("ABC", "Foo");
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "TestTree2");
				tree.Add("ABC", "Foo");
				tx.Commit();
			}

			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				TransactionToShip tx;
				transactionsToShip.TryDequeue(out tx);
				shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot, tx.PreviousTransactionCrc);

				transactionsToShip.TryDequeue(out tx);
				shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot, tx.PreviousTransactionCrc);

				using (var snaphsot = shippingDestinationEnv.CreateSnapshot())
				{
					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo", result.Reader.ToStringValue());
					});

					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree2", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo", result.Reader.ToStringValue());
					});
				}
			}
		}

		[Fact]
		public void StorageEnvironment_Two_Different_Tx_With_env_shutdown_Should_be_shipped_properly()
		{
			var transactionsToShip = new ConcurrentQueue<TransactionToShip>();
			Env.Journal.OnTransactionCommit += tx =>
			{
				tx.CreatePagesSnapshot();
				transactionsToShip.Enqueue(tx);
			};

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "TestTree");
				tree.Add("ABC", "Foo");
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "TestTree2");
				tree.Add("ABC", "Foo");
				tx.Commit();
			}

			var tempPath = "Temp" + Guid.NewGuid();
			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.ForPath(tempPath)))
			{
				TransactionToShip tx;
				transactionsToShip.TryDequeue(out tx);
				shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot, tx.PreviousTransactionCrc);

				using (var snaphsot = shippingDestinationEnv.CreateSnapshot())
				{
					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo", result.Reader.ToStringValue());
					});
				}
			}

			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.ForPath(tempPath)))
			{
				TransactionToShip tx;

				transactionsToShip.TryDequeue(out tx);
				shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot, tx.PreviousTransactionCrc);

				using (var snaphsot = shippingDestinationEnv.CreateSnapshot())
				{
					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo", result.Reader.ToStringValue());
					});

					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree2", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo", result.Reader.ToStringValue());
					});
				}
			}
		}

		[Fact]
		public void StorageEnvironment_CreateTree_Should_be_shipped_properly()
		{
			var transactionsToShip = new ConcurrentBag<TransactionToShip>();
			Env.Journal.OnTransactionCommit += tx =>
			{
				tx.CreatePagesSnapshot();
				transactionsToShip.Add(tx);
			};

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "TestTree");
				tree.Add("ABC", "Foo");
				tx.Commit();				
			}

			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				foreach (var tx in transactionsToShip)
					shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot, tx.PreviousTransactionCrc);

				using (var snaphsot = shippingDestinationEnv.CreateSnapshot())
				{
					Assert.DoesNotThrow(() => //if tree doesn't exist --> throws InvalidOperationException
					{
						var result = snaphsot.Read("TestTree", "ABC");
						Assert.Equal(1, result.Version);
						Assert.Equal("Foo",result.Reader.ToStringValue());
					});
				}
			}
		}

		[Fact]
		public void StorageEnvironment_should_be_able_to_accept_transactionsToShip()
		{
			var transactionsToShip = new ConcurrentBag<TransactionToShip>();
			Env.Journal.OnTransactionCommit += tx =>
			{
				tx.CreatePagesSnapshot();
				transactionsToShip.Add(tx);
			};

			WriteTestDataToEnv();
			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				foreach (var tx in transactionsToShip.OrderBy(x => x.Header.TransactionId))
					shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot, tx.PreviousTransactionCrc);
				using (var snapshot = shippingDestinationEnv.CreateSnapshot())
				{
					var fooReadResult = snapshot.Read("TestTree", "foo");
					Assert.NotNull(fooReadResult);

					var fooValue = Encoding.UTF8.GetString(fooReadResult.Reader.AsStream().ReadData());
					Assert.Equal("bar", fooValue);

					var barReadResult = snapshot.Read("TestTree", "bar");
					Assert.NotNull(barReadResult);

					var barValue = Encoding.UTF8.GetString(barReadResult.Reader.AsStream().ReadData());
					Assert.Equal("foo", barValue);
				}
			}
		}

		[Fact]
		public void StorageEnvironment_should_be_able_to_accept_transactionsToShip_with_LOTS_of_transactions()
		{
			var transactionsToShip = new ConcurrentBag<TransactionToShip>();
			using (var shippingSourceEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				shippingSourceEnv.Journal.OnTransactionCommit += tx =>
				{
					tx.CreatePagesSnapshot();
					transactionsToShip.Add(tx);
				};

				using (var tx = shippingSourceEnv.NewTransaction(TransactionFlags.ReadWrite))
				{
					shippingSourceEnv.CreateTree(tx, "TestTree");
					shippingSourceEnv.CreateTree(tx, "TestTree2");
					tx.Commit();
				}

				WriteLotsOfTestDataForTree("TestTree", shippingSourceEnv);
				WriteLotsOfTestDataForTree("TestTree2", shippingSourceEnv);
			}

			var storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
			storageEnvironmentOptions.ManualFlushing = true;
			using (var shippingDestinationEnv = new StorageEnvironment(storageEnvironmentOptions))
			{
				foreach (var tx in transactionsToShip.OrderBy(x => x.Header.TransactionId))
					Assert.DoesNotThrow(() => shippingDestinationEnv.Journal.Shipper.ApplyShippedLog(tx.PagesSnapshot,tx.PreviousTransactionCrc));

				shippingDestinationEnv.FlushLogToDataFile();

				using (var snapshot = shippingDestinationEnv.CreateSnapshot())
				{
					ValidateLotsOfTestDataForTree(snapshot, "TestTree");
					ValidateLotsOfTestDataForTree(snapshot, "TestTree2");
				}
			}
		}

		private void ValidateLotsOfTestDataForTree(SnapshotReader snapshot, string treeName)
		{
			for (int i = 0; i < 50; i++)
			{
				for (int j = 0; j < 500; j++)
				{
					var index = (i + "/ " + j);
					var key = "key/" + index;
					var expectedValue = "value/" + index;
					var result = snapshot.Read(treeName, key);

					Assert.NotNull(result);
					var fetchedValue = Encoding.UTF8.GetString(result.Reader.AsStream().ReadData());

					Assert.Equal(expectedValue, fetchedValue);
				}
			}
		}

		private void WriteLotsOfTestDataForTree(string treeName, StorageEnvironment storageEnvironment)
		{
			for (int i = 0; i < 50; i++)
			{
				using (var writeBatch = new WriteBatch())
				{
					for (int j = 0; j < 500; j++)
					{
						var index = (i + "/ " + j);
						writeBatch.Add("key/" + index, StreamFor("value/" + index), treeName);
					}

					storageEnvironment.Writer.Write(writeBatch);
				}
			}
		}


		private void WriteTestDataToEnv()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "TestTree");
				tx.Commit();
			}

			var writeBatch = new WriteBatch();
			writeBatch.Add("foo", StreamFor("bar"), "TestTree");

			Env.Writer.Write(writeBatch);

			writeBatch = new WriteBatch();
			writeBatch.Add("bar", StreamFor("foo"), "TestTree");

			Env.Writer.Write(writeBatch);
		}
	}
}

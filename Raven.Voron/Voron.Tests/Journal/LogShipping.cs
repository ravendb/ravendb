using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;
using Xunit;

namespace Voron.Tests.Journal
{
	public class LogShipping : StorageTest
	{
		public LogShipping()
			: base(StorageEnvironmentOptions.CreateMemoryOnly())
		{
		}

		[Fact]
		public unsafe void Committing_tx_should_fire_event_with_transactionsToShip_records()
		{
			var transactionsToShip = new List<TransactionToShip>();
			Env.Journal.OnTransactionCommit += ship =>
			{
			    ship.CopyPages();
			    transactionsToShip.Add(ship);   
			};

			WriteTestDataToEnv();

			Assert.Equal(3, transactionsToShip.Count);

			//validate crc
			foreach (var tx in transactionsToShip)
			{
				var compressedDataBuffer = tx.CopiedPages;
				fixed (byte* compressedDataBufferPtr = compressedDataBuffer)
				{
					var crc = Crc.Value(compressedDataBufferPtr, 0, compressedDataBuffer.Length);
					Assert.Equal(tx.Header.Crc, crc);
				}
			}
		}

		[Fact]
		public void StorageEnvironment_should_be_able_to_accept_transactionsToShip()
		{
			var transactionsToShip = new ConcurrentBag<TransactionToShip>();
			Env.Journal.OnTransactionCommit += transactionsToShip.Add;

			WriteTestDataToEnv();
			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				shippingDestinationEnv.Journal.Shipper.ApplyShippedLogs(transactionsToShip);
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
			Env.Journal.OnTransactionCommit += transactionsToShip.Add;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "TestTree");
				Env.CreateTree(tx, "TestTree2");
				tx.Commit();
			}

			WriteLotsOfTestDataForTree("TestTree");
			WriteLotsOfTestDataForTree("TestTree2");

			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				Assert.DoesNotThrow(() => shippingDestinationEnv.Journal.Shipper.ApplyShippedLogs(transactionsToShip));

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

		private void WriteLotsOfTestDataForTree(string treeName)
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

					Env.Writer.Write(writeBatch);
				}
			}
		}

		[Fact]
		public void Committed_tx_should_be_possible_to_read_from_journal_as_shipping_records()
		{
			var transactionsToShipFromCommits = new ConcurrentBag<TransactionToShip>();
			Env.Journal.OnTransactionCommit += ship =>
			{
                ship.CopyPages();
			    transactionsToShipFromCommits.Add(ship);
			};

			WriteTestDataToEnv();

			//will read 4 transactions --> 
			//the 3 that were written in WriteTestDataToEnv() and the "create new database" transaction
			var transactionsToShip = Env.Journal.Shipper.ReadJournalForShippings(-1).ToList();

			Assert.Equal(4, transactionsToShip.Count);
			Assert.Equal((uint)0, transactionsToShip[0].PreviousTransactionCrc);
			Assert.Equal(transactionsToShip[0].Header.Crc, transactionsToShip[1].PreviousTransactionCrc);
			Assert.Equal(transactionsToShip[1].Header.Crc, transactionsToShip[2].PreviousTransactionCrc);
			Assert.Equal(transactionsToShip[2].Header.Crc, transactionsToShip[3].PreviousTransactionCrc);

			var dataPairs = (from txFromCommit in transactionsToShipFromCommits
							 join txFromRead in transactionsToShip on txFromCommit.Header.TransactionId equals txFromRead.Header.TransactionId
							 select Tuple.Create(txFromCommit, txFromRead)).ToList();

			dataPairs.ForEach(pair => Assert.Equal(pair.Item1.CopiedPages, pair.Item2.CopiedPages));

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

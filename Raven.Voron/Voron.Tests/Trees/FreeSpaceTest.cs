using System;
using System.Collections.Generic;
using System.IO;
using Voron.Impl.FreeSpace;
using Xunit;

namespace Voron.Tests.Trees
{
	public class FreeSpaceTest : StorageTest
	{
		[Fact]
		public void WillBeReused()
		{
			var random = new Random();
			var buffer = new byte[512];
			random.NextBytes(buffer);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 25; i++)
				{
					tx.State.Root.Add(tx, i.ToString("0000"), new MemoryStream(buffer));
				}

				tx.Commit();
			}
			var before = Env.Stats();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 25; i++)
				{
					tx.State.Root.Delete(tx, i.ToString("0000"));
				}

				tx.Commit();
			}

			var old = Env.NextPageNumber;
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 25; i++)
				{
					tx.State.Root.Add(tx, i.ToString("0000"), new MemoryStream(buffer));
				}

				tx.Commit();
			}

			var after = Env.Stats();

			Assert.Equal(after.RootPages, before.RootPages);

			Assert.True(Env.NextPageNumber - old < 2, "This test will not pass until we finish merging the free space branch");
		}

		[Fact]
		public void ShouldReturnProperPageFromSecondSection()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.FreePage(tx, FreeSpaceHandling.NumberOfPagesInSection + 1);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Assert.Equal(FreeSpaceHandling.NumberOfPagesInSection + 1, Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx, 1));
			}
		}

		[Fact]
		public void CanReuseMostOfFreePages_RemainingOnesCanBeTakenToHandleFreeSpace()
		{
			const int maxPageNumber = 4000000;
			const int numberOfFreedPages = 100;
			var random = new Random(3);
			var freedPages = new HashSet<long>();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.NextPageNumber = maxPageNumber + 1;

				tx.Commit();
			}

			for (int i = 0; i < numberOfFreedPages; i++)
			{
				long pageToFree;
				do
				{
					pageToFree = random.Next(0, maxPageNumber);
				} while (freedPages.Add(pageToFree) == false);

				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					Env.FreeSpaceHandling.FreePage(tx, pageToFree);

					tx.Commit();
				}
			}

			// we cannot expect that all freed pages will be available for a reuse
			// some freed pages can be used internally by free space handling
			// 80% should be definitely a safe value

			var minNumberOfFreePages = numberOfFreedPages * 0.8;

			for (int i = 0; i < minNumberOfFreePages; i++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var page = Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx, 1);

					Assert.NotNull(page);
					Assert.True(freedPages.Remove(page.Value));

					tx.Commit();
				}
			}
		}

		[Fact]
		public void FreeSpaceHandlingShouldNotReturnPagesThatAreAlreadyAllocated()
		{
			const int maxPageNumber = 400000;
			const int numberOfFreedPages = 60;
			var random = new Random(2);
			var freedPages = new HashSet<long>();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.NextPageNumber = maxPageNumber + 1;

				tx.Commit();
			}

			for (int i = 0; i < numberOfFreedPages; i++)
			{
				long pageToFree;
				do
				{
					pageToFree = random.Next(0, maxPageNumber);
				} while (freedPages.Add(pageToFree) == false);

				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					Env.FreeSpaceHandling.FreePage(tx, pageToFree);

					tx.Commit();
				}
			}

			var alreadyReused = new List<long>();

			do
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var page = Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx, 1);

					if (page == null)
					{
						break;
					}

					Assert.False(alreadyReused.Contains(page.Value), "Free space handling returned a page number that has been already allocated. Page number: " + page);
					Assert.True(freedPages.Remove(page.Value));

					alreadyReused.Add(page.Value);

					tx.Commit();
				}
			} while (true);
		}
	}
}
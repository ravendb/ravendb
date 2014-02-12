using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Storage
{
	public class MemoryMapWithoutBackingPagerTest : StorageTest
	{
		private const string TestTreeName = "tree";
		private const long PagerInitialSize = 64 * 1024;
		public MemoryMapWithoutBackingPagerTest()
			: base(StorageEnvironmentOptions.GetInMemory("TestSystemPagingMMF"))
		{			
		}

		[Fact]
		public void Should_be_able_to_read_and_write()
		{
			CreatTestSchema();
			var writeBatch = new WriteBatch();
			writeBatch.Add("key",StreamFor("value"),TestTreeName);
			Env.Writer.Write(writeBatch);

			using (var snapshot = Env.CreateSnapshot())
			{
				var storedValue = Encoding.UTF8.GetString(snapshot.Read(TestTreeName, "key").Reader.AsStream().ReadData());
				Assert.Equal("value",storedValue);
			}
		}


		[Fact]
		public void Should_be_able_to_allocate_new_pages_once()
		{
			var numberOfPagesBeforeAllocation = Env.Options.DataPager.NumberOfAllocatedPages;
			Assert.DoesNotThrow(() => Env.Options.DataPager.AllocateMorePages(null,PagerInitialSize * 2));
			Assert.Equal(numberOfPagesBeforeAllocation * 2, Env.Options.DataPager.NumberOfAllocatedPages);
		}

		private void CreatTestSchema()
		{
			using(var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, TestTreeName);
				tx.Commit();
			}
		}

		[Fact]
		public void Should_be_able_to_allocate_new_pages_multiple_times()
		{
			var pagerSize = PagerInitialSize;
			for (int allocateMorePagesCount = 0; allocateMorePagesCount < 5; allocateMorePagesCount++)
			{
				pagerSize *= 2;
				var numberOfPagesBeforeAllocation = Env.Options.DataPager.NumberOfAllocatedPages;
				Assert.DoesNotThrow(() => Env.Options.DataPager.AllocateMorePages(null, pagerSize));
				Assert.Equal(numberOfPagesBeforeAllocation*2, Env.Options.DataPager.NumberOfAllocatedPages);
			}
		}
	}
}

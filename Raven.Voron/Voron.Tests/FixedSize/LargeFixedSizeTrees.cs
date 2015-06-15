// -----------------------------------------------------------------------
//  <copyright file="LargeFixedSizeTrees.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Voron.Util.Conversion;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.FixedSize
{
	public class LargeFixedSizeTrees : StorageTest
	{
		
        [Theory]
		[InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024 * 256)]
        public void CanAdd_ALot_ForPageSplits(int count)
        {
	        var bytes = new byte[48];
	        var slice = new Slice(bytes);
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 0; i < count; i++)
                {
	                EndianBitConverter.Little.CopyBytes(i, bytes, 0);
					fst.Add(i, slice);
					Assert.NotNull(fst.Read(i));
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
				var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 0; i < count; i++)
                {
					Assert.True(fst.Contains(i));
	                var read = fst.Read(i);
	                read.CopyTo(bytes);
	                Assert.Equal(i, EndianBitConverter.Little.ToInt32(bytes, 0));
                }
	            tx.Commit();
            }
        }

		[Theory]
        [InlineData(8)]
        [InlineData(16)]
		[InlineData(128)]
        [InlineData(1024*256)]
		public void CanRemove_ALot_ForPageSplits(int count)
		{
			var bytes = new byte[48];
			var slice = new Slice(bytes);
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

				for (int i = 0; i < count; i++)
				{
					EndianBitConverter.Little.CopyBytes(i, bytes, 0);
					fst.Add(i, slice);
				}

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

				for (int i = 0; i < count; i++)
				{
					fst.Delete(i);
				}

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

				for (int i = 0; i < count; i++)
				{
					Assert.False(fst.Contains(i), i.ToString());
				}
				tx.Commit();
			}
		}


	}
}
// -----------------------------------------------------------------------
//  <copyright file="MultiTreeSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests
{
	public class MultiTreeSize : StorageTest
	{
		[Fact]
		public void Single_AddMulti_WillUseOnePage()
		{
			using (var tx = Env.WriteTransaction())
			{
			    tx.CreateTree("foo");
			    tx.Commit();
			}
            var usedDataFileSizeInBytes = Env.Stats().UsedDataFileSizeInBytes;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.MultiAdd("ChildTreeKey", "test");
                tx.Commit();
            }

		    Assert.Equal(0,usedDataFileSizeInBytes - Env.Stats().UsedDataFileSizeInBytes);
		}

		[Fact]
		public void TwoSmall_AddMulti_WillUseOnePage()
		{
			using (var tx = Env.WriteTransaction())
			{
                tx.CreateTree("foo");
				tx.Commit();
			}

            var usedDataFileSizeInBytes = Env.Stats().UsedDataFileSizeInBytes;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.MultiAdd("ChildTreeKey", "test1");
                tree.MultiAdd("ChildTreeKey", "test2");
                tx.Commit();
            }

            Assert.Equal(0, usedDataFileSizeInBytes - Env.Stats().UsedDataFileSizeInBytes);

        }
    }
}
// -----------------------------------------------------------------------
//  <copyright file="LargeFixedSizeTrees.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.FixedSize
{
	public class LargeFixedSizeTrees : StorageTest
	{
		
        [Theory]
        [InlineData(80)]
        [InlineData(800)]
        [InlineData(8000)]
        public void CanAdd_ALot_ForPageSplits(int count)
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                for (int i = 0; i < count; i++)
                {
                    fst.Add(i);
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                for (int i = 0; i < count; i++)
                {
                    Assert.True(fst.Contains(i), i.ToString());
                }
                tx.Commit();
            }
        }
	}
}
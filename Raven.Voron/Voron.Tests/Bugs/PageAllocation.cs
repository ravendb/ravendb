// -----------------------------------------------------------------------
//  <copyright file="SomeIssue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Xunit;

namespace Voron.Tests.Bugs
{
    public class PageAllocation : StorageTest
    {
        /// <summary>
        /// http://issues.hibernatingrhinos.com/issue/RavenDB-1707
        /// </summary>
        [Fact]
        public void MultipleTxPagesCanPointToOnePageNumberWhichShouldNotBeCausingIssuesDuringFlushing()
        {
            var options = StorageEnvironmentOptions.GetInMemory();
            options.ManualFlushing = true;
            using (var env = new StorageEnvironment(options))
            {
                var trees = CreateTrees(env, 2, "tree");
                var tree1 = trees[0];
                var tree2 = trees[1];

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var t1 = tx.State.GetTree(tx, tree1);

                    t1.MultiAdd(tx, "key", "value/1");
                    t1.MultiAdd(tx, "key", "value/2");

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var t1 = tx.State.GetTree(tx, tree1);
                    var t2 = tx.State.GetTree(tx, tree2);

                    var buffer = new byte[1000];

                    t1.MultiDelete(tx, "key", "value/1");
                    t1.MultiDelete(tx, "key", "value/2");

                    t2.Add(tx, "key/1", new MemoryStream(buffer));
                    t2.Add(tx, "key/2", new MemoryStream(buffer));
                    t2.Add(tx, "key/3", new MemoryStream(buffer));
                    t2.Add(tx, "key/4", new MemoryStream(buffer));
                    t2.Add(tx, "key/5", new MemoryStream(buffer));

                    tx.Commit();
                }

                env.FlushLogToDataFile();
            }
        }
    }
}
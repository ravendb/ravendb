// -----------------------------------------------------------------------
//  <copyright file="SomeIssue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class PageAllocation : FastTests.Voron.StorageTest
    {
        public PageAllocation(ITestOutputHelper output) : base(output)
        {
        }

        /// <summary>
        /// https://issues.hibernatingrhinos.com/issue/RavenDB-1707
        /// </summary>
        [Fact]
        public void MultipleTxPagesCanPointToOnePageNumberWhichShouldNotBeCausingIssuesDuringFlushing()
        {
            var options = StorageEnvironmentOptions.CreateMemoryOnlyForTests();
            options.ManualFlushing = true;
            using (var env = new StorageEnvironment(options))
            {
                var trees = CreateTrees(env, 2, "tree");
                var tree1 = trees[0];
                var tree2 = trees[1];

                using (var tx = env.WriteTransaction())
                {
                    var t1 = tx.ReadTree(tree1);

                    t1.MultiAdd("key", "value/1");
                    t1.MultiAdd("key", "value/2");

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var t1 = tx.ReadTree(tree1);
                    var t2 = tx.ReadTree(tree2);

                    var buffer = new byte[1000];

                    t1.MultiDelete("key", "value/1");
                    t1.MultiDelete("key", "value/2");

                    t2.Add("key/1", new MemoryStream(buffer));
                    t2.Add("key/2", new MemoryStream(buffer));
                    t2.Add("key/3", new MemoryStream(buffer));
                    t2.Add("key/4", new MemoryStream(buffer));
                    t2.Add("key/5", new MemoryStream(buffer));

                    tx.Commit();
                }

                env.FlushLogToDataFile();
            }
        }
    }
}

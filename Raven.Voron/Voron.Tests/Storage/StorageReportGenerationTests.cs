// -----------------------------------------------------------------------
//  <copyright file="Stats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Trees;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.Storage
{
    public class StorageReportGenerationTests : StorageTest
    {
        [Fact]
        public void AllocatedSpaceOfDataFileEqualsToSumOfSpaceInUseAndFreeSpace()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var report = Env.GenerateReport(tx, false, msg => { }, CancellationToken.None);

                Assert.Equal(report.DataFile.AllocatedSpaceInBytes, report.DataFile.SpaceInUseInBytes + report.DataFile.FreeSpaceInBytes);
            }
        }

        [PrefixesTheory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(13)]
        public void TreeReportsContainCompleteInformationAboutAllExistingTrees(int numberOfTrees)
        {
            var r = new Random();
            var numberOfOverflowPages = new Dictionary<string, long>();
            var numberOfEntries = new Dictionary<string, long>();

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < numberOfTrees; i++)
                {
                    var tree = Env.CreateTree(tx, "tree_" + i);

                    var entries = AddEntries(tree, i).Count;
                    var overflows = AddOverflows(tx, tree, i, r);

                    numberOfEntries.Add(tree.Name, entries + overflows.AddedEntries.Count);
                    numberOfOverflowPages.Add(tree.Name, overflows.NumberOfOverflowPages);
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var report = Env.GenerateReport(tx, true, msg => { }, CancellationToken.None);

                Assert.Equal(report.DataFile.AllocatedSpaceInBytes, report.DataFile.SpaceInUseInBytes + report.DataFile.FreeSpaceInBytes);
                Assert.Equal(numberOfTrees, report.Trees.Count);

                foreach (var treeReport in report.Trees)
                {
                    Assert.True(treeReport.PageCount > 0);
                    Assert.Equal(treeReport.PageCount, treeReport.BranchPages + treeReport.LeafPages + treeReport.OverflowPages);

                    Assert.Equal(numberOfOverflowPages[treeReport.Name], treeReport.OverflowPages);
                    
                    Assert.Equal(numberOfEntries[treeReport.Name], treeReport.EntriesCount);

                    Assert.True(treeReport.Density > 0 && treeReport.Density <= 1.0);
                }
            }
        }

        [PrefixesTheory]
        [InlineData(0)]
        [InlineData(7)]
        [InlineData(14)]
        public void TreeReportsAreEmptyAfterRemovingAllEntries(int numberOfTrees)
        {
            var addedEntries = new Dictionary<string, List<string>>();

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var r = new Random();

                for (int i = 0; i < numberOfTrees; i++)
                {
                    var tree = Env.CreateTree(tx, "tree_" + i);

                    var entries = AddEntries(tree, i);
                    var overflows = AddOverflows(tx, tree, i, r);

                    addedEntries.Add(tree.Name, entries.Union(overflows.AddedEntries).ToList());
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                foreach (var entries in addedEntries)
                {
                    var tree = tx.ReadTree(entries.Key);

                    foreach (var key in entries.Value)
                    {
                        tree.Delete(key);
                    }
                }
                
                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var report = Env.GenerateReport(tx, true, msg => { }, CancellationToken.None);

                Assert.Equal(report.DataFile.AllocatedSpaceInBytes, report.DataFile.SpaceInUseInBytes + report.DataFile.FreeSpaceInBytes);
                Assert.Equal(numberOfTrees, report.Trees.Count);

                foreach (var treeReport in report.Trees)
                {
                    Assert.Equal(1, treeReport.PageCount); // root
                    Assert.Equal(0, treeReport.BranchPages);
                    Assert.Equal(1, treeReport.LeafPages); // root
                    Assert.Equal(0, treeReport.OverflowPages);
                    Assert.Equal(0, treeReport.EntriesCount);
                    Assert.True(treeReport.Density > 0 && treeReport.Density <= 1.0); // just the header of page root
                    Assert.Equal(1, treeReport.Depth);
                }
            }
        }

        [Theory]
        [InlineData(new []{1.0, 1.0, 1.0}, 1.0)]
        [InlineData(new [] { 1.0, 0.0 }, 0.5)]
        [InlineData(new[] { 0.8, 0.7, 0.9, 0.8 }, 0.8)]
        [InlineData(new[] { 0.3, 0.5, 0.1, 0.3 }, 0.3)]
        public void ReportGeneratorCalculatesCorrectDensity(double[] pageDensities, double expectedDensity)
        {
            Assert.Equal(expectedDensity, StorageReportGenerator.CalculateTreeDensity(pageDensities.ToList()));
        }

        [PrefixesTheory]
        [InlineData(0)]
        [InlineData(7)]
        [InlineData(14)]
        public void JournalReportsArePresent(int numberOfTrees)
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < numberOfTrees; i++)
                {
                    var tree = Env.CreateTree(tx, "tree_" + i);

                    AddEntries(tree, i);
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var report = Env.GenerateReport(tx, false, msg => { }, CancellationToken.None);

                Assert.NotEmpty(report.Journals);

                foreach (var journalReport in report.Journals)
                {
                    Assert.True(journalReport.Number >=0);
                    Assert.True(journalReport.AllocatedSpaceInBytes > 0);
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(15)]
        public void TreeReportContainsInfoAboutPagesUsedByFixedSizeTrees(int numberOfFixedSizeTrees)
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "fixed-size-trees");

                for (int treeNumber = 0; treeNumber < numberOfFixedSizeTrees; treeNumber++)
                {
                    var r = new Random();
                    byte valueSize = (byte) r.Next(byte.MaxValue);

                    var fst = tree.FixedTreeFor("test-" + treeNumber, valueSize);

                    for (int i = 0; i < r.Next(1000); i++)
                    {
                        if(valueSize == 0)
                            fst.Add(i);
                        else 
                            fst.Add(i, new byte[valueSize]);
                    }
                }
                
                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var report = Env.GenerateReport(tx, true, msg => { }, CancellationToken.None);

                Assert.Equal(report.DataFile.AllocatedSpaceInBytes, report.DataFile.SpaceInUseInBytes + report.DataFile.FreeSpaceInBytes);
                Assert.Equal(1, report.Trees.Count);

                var treeReport = report.Trees[0];

                Assert.True(treeReport.PageCount > 0);
                Assert.Equal(treeReport.PageCount, treeReport.BranchPages + treeReport.LeafPages);
                Assert.Equal(numberOfFixedSizeTrees, treeReport.EntriesCount);
                Assert.True(treeReport.Density > 0 && treeReport.Density <= 1.0); // just the header of page root
            }
        }

        [Theory]
        [InlineData(new[] { "key" }, 1000)]
        [InlineData(new[] { "key1", "key2" }, 1000)]
        [InlineData(new[] { "key" }, 1)]
        [InlineData(new[] { "key1", "key2" }, 2)]
        [InlineData(new[] { "key" }, 30)]
        [InlineData(new[] { "key1", "key2" }, 30)]
        public void TreeReportContainsInformationAboutMultiValueEntries(string[] keys, int numberOfValuesPerKey)
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "multi-tree");

                foreach (var key in keys)
                {
                    for (int i = 0; i < numberOfValuesPerKey; i++)
                    {
                        tree.MultiAdd(key, "items/" + i + "/" + new string('p', i));
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var report = Env.GenerateReport(tx, true, msg => { }, CancellationToken.None);

                Assert.Equal(keys.Length, report.Trees[0].EntriesCount);

                Assert.Equal(keys.Length * numberOfValuesPerKey, report.Trees[0].MultiValues.EntriesCount);
            }
        }

        [Fact]
        public void TreeReportContainsInformationAboutPagesOfChildMultiTrees()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "multi-tree");

                for (int i = 0; i < 1000; i++)
                {
                    tree.MultiAdd("key", "items/" + i + "/" + new string('p', i));
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var report = Env.GenerateReport(tx, true, msg => { }, CancellationToken.None);

                Assert.True(report.Trees[0].MultiValues.PageCount > 0);
                Assert.Equal(report.Trees[0].MultiValues.PageCount, report.Trees[0].MultiValues.LeafPages + report.Trees[0].MultiValues.BranchPages + report.Trees[0].MultiValues.OverflowPages);
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("multi-tree");

                for (int i = 0; i < 1000; i++)
                {
                    tree.MultiDelete("key", "items/" + i + "/" + new string('p', i));
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var report = Env.GenerateReport(tx, true, msg => { }, CancellationToken.None);

                Assert.True(report.Trees[0].MultiValues.PageCount == 0);
                Assert.Equal(report.Trees[0].MultiValues.PageCount, report.Trees[0].MultiValues.LeafPages + report.Trees[0].MultiValues.BranchPages + report.Trees[0].MultiValues.OverflowPages);
            }
        }

        private List<string> AddEntries(Tree tree, int treeNumber)
        {
            var entriesAdded = new List<string>();

            for (int j = 0; j < treeNumber; j++)
            {
                string key = "value_" + j;
                tree.Add(key, new MemoryStream(new byte[128]));
                entriesAdded.Add(key);
            }

            return entriesAdded;
        }

        class OverflowsAddResult
        {
            public List<string> AddedEntries;
            public long NumberOfOverflowPages;
        }

        private OverflowsAddResult AddOverflows(Transaction tx, Tree tree, int treeNumber, Random r)
        {
            var minOverflowSize = AbstractPager.NodeMaxSize - Constants.NodeHeaderSize + 1;
            var entriesAdded = new List<string>();
            var overflowsAdded = 0;

            for (int j = 0; j < treeNumber; j++)
            {
                var overflowSize = r.Next(minOverflowSize, 10000);
                string key = "overflow_" + j;
                tree.Add(key, new MemoryStream(new byte[overflowSize]));

                entriesAdded.Add(key);
                overflowsAdded += tx.DataPager.GetNumberOfOverflowPages(overflowSize);
            }

            return new OverflowsAddResult
            {
                AddedEntries = entriesAdded,
                NumberOfOverflowPages = overflowsAdded
            };
        } 
    }
}

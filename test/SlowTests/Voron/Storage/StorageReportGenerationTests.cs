// -----------------------------------------------------------------------
//  <copyright file="Stats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron.FixedSize;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class StorageReportGenerationTests : FastTests.Voron.StorageTest
    {
        [Fact]
        public void AllocatedSpaceOfDataFileEqualsToSumOfSpaceInUseAndFreeSpace()
        {
            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx);

                Assert.Equal(report.DataFile.AllocatedSpaceInBytes, report.DataFile.UsedSpaceInBytes + report.DataFile.FreeSpaceInBytes);
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(13)]
        public void TreeReportsContainCompleteInformationAboutAllExistingTrees(int numberOfTrees)
        {
            var r = new Random();
            var numberOfOverflowPages = new Dictionary<string, long>();
            var numberOfEntries = new Dictionary<string, long>();

            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < numberOfTrees; i++)
                {
                    var tree = tx.CreateTree("tree_" + i);

                    var entries = AddEntries(tree, i).Count;
                    var overflows = AddOverflows(tx, tree, i, r);

                    numberOfEntries.Add(tree.Name.ToString(), entries + overflows.AddedEntries.Count);
                    numberOfOverflowPages.Add(tree.Name.ToString(), overflows.NumberOfOverflowPages);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);

                Assert.Equal(report.DataFile.AllocatedSpaceInBytes, report.DataFile.UsedSpaceInBytes + report.DataFile.FreeSpaceInBytes);
                Assert.Equal(numberOfTrees + 1/*$Database-Metadata*/, report.Trees.Count);

                foreach (var treeReport in report.Trees)
                {
                    if (string.Equals(treeReport.Name, Constants.MetadataTreeNameSlice.ToString()))
                        continue;

                    Assert.True(treeReport.PageCount > 0);
                    Assert.Equal(treeReport.PageCount, treeReport.BranchPages + treeReport.LeafPages + treeReport.OverflowPages);

                    Assert.Equal(numberOfOverflowPages[treeReport.Name.ToString()], treeReport.OverflowPages);

                    Assert.Equal(numberOfEntries[treeReport.Name.ToString()], treeReport.NumberOfEntries);

                    Assert.True(treeReport.Density > 0 && treeReport.Density <= 1.0);
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(7)]
        [InlineData(14)]
        public void TreeReportsAreEmptyAfterRemovingAllEntries(int numberOfTrees)
        {
            var addedEntries = new Dictionary<string, List<string>>();

            using (var tx = Env.WriteTransaction())
            {
                var r = new Random();

                for (int i = 0; i < numberOfTrees; i++)
                {
                    var tree = tx.CreateTree("tree_" + i);

                    var entries = AddEntries(tree, i);
                    var overflows = AddOverflows(tx, tree, i, r);

                    addedEntries.Add(tree.Name.ToString(), entries.Union(overflows.AddedEntries).ToList());
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
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

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);

                Assert.Equal(report.DataFile.AllocatedSpaceInBytes, report.DataFile.UsedSpaceInBytes + report.DataFile.FreeSpaceInBytes);
                Assert.Equal(numberOfTrees + 1/*$Database-Metadata*/, report.Trees.Count);

                foreach (var treeReport in report.Trees)
                {
                    if (string.Equals(treeReport.Name, Constants.MetadataTreeNameSlice.ToString()))
                        continue;

                    Assert.Equal(1, treeReport.PageCount); // root
                    Assert.Equal(0, treeReport.BranchPages);
                    Assert.Equal(1, treeReport.LeafPages); // root
                    Assert.Equal(0, treeReport.OverflowPages);
                    Assert.Equal(0, treeReport.NumberOfEntries);
                    Assert.True(treeReport.Density > 0 && treeReport.Density <= 1.0); // just the header of page root
                    Assert.Equal(1, treeReport.Depth);
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(7)]
        [InlineData(14)]
        public void JournalReportsArePresent(int numberOfTrees)
        {
            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < numberOfTrees; i++)
                {
                    var tree = tx.CreateTree("tree_" + i);

                    AddEntries(tree, i);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx);

                Assert.NotEmpty(report.Journals);

                foreach (var journalReport in report.Journals)
                {
                    Assert.True(journalReport.Number >= 0);
                    Assert.True(journalReport.AllocatedSpaceInBytes > 0);
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(7)]
        [InlineData(14)]
        public void TempBuffersReportsArePresent(int numberOfTrees)
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < numberOfTrees; i++)
                {
                    var tree = tx.CreateTree("tree_" + i);

                    AddEntries(tree, i);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx);

                Assert.NotEmpty(report.TempBuffers);

                foreach (var tempBuffer in report.TempBuffers)
                {
                    Assert.NotNull(tempBuffer.Name);
                    Assert.True(tempBuffer.AllocatedSpaceInBytes > 0);
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(15)]
        public void TreeReportContainsInfoAboutPagesUsedByFixedSizeTrees(int numberOfFixedSizeTrees)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("fixed-size-trees");

                for (int treeNumber = 0; treeNumber < numberOfFixedSizeTrees; treeNumber++)
                {
                    var r = new Random(numberOfFixedSizeTrees);
                    byte valueSize = (byte)r.Next(byte.MaxValue);

                    var fst = tree.FixedTreeFor("test-" + treeNumber, valueSize);

                    for (int i = 0; i < r.Next(1000); i++)
                    {
                        if (valueSize == 0)
                            fst.Add(i);
                        else
                            fst.Add(i, new byte[valueSize]);
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);

                Assert.Equal(report.DataFile.AllocatedSpaceInBytes, report.DataFile.UsedSpaceInBytes + report.DataFile.FreeSpaceInBytes);
                Assert.Equal(1 + 1/*$Database-Metadata*/, report.Trees.Count);

                var treeReport = report.Trees[1];

                Assert.True(treeReport.PageCount > 0);
                Assert.Equal(treeReport.PageCount, treeReport.BranchPages + treeReport.LeafPages);
                Assert.Equal(numberOfFixedSizeTrees, treeReport.NumberOfEntries);
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
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("multi-tree");

                foreach (var key in keys)
                {
                    for (int i = 0; i < numberOfValuesPerKey; i++)
                    {
                        tree.MultiAdd(key, "items/" + i + "/" + new string('p', i));
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);

                Assert.Equal(keys.Length, report.Trees[1].NumberOfEntries);

                Assert.Equal(keys.Length * numberOfValuesPerKey, report.Trees[1].MultiValues.NumberOfEntries);
            }
        }

        [Fact]
        public void TreeReportContainsInformationAboutPagesOfChildMultiTrees()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("multi-tree");

                for (int i = 0; i < 1000; i++)
                {
                    tree.MultiAdd("key", "items/" + i + "/" + new string('p', i));
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);

                Assert.True(report.Trees[1].MultiValues.PageCount > 0);
                Assert.Equal(report.Trees[1].MultiValues.PageCount, 
                    report.Trees[1].MultiValues.LeafPages + report.Trees[1].MultiValues.BranchPages + report.Trees[1].MultiValues.OverflowPages);
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("multi-tree");

                for (int i = 0; i < 1000; i++)
                {
                    tree.MultiDelete("key", "items/" + i + "/" + new string('p', i));
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);

                Assert.True(report.Trees[1].MultiValues.PageCount == 0);
                Assert.Equal(report.Trees[1].MultiValues.PageCount, 
                    report.Trees[1].MultiValues.LeafPages + report.Trees[1].MultiValues.BranchPages + report.Trees[1].MultiValues.OverflowPages);
            }
        }

        [Theory]
        [InlineDataWithRandomSeed(1)]
        [InlineDataWithRandomSeed(5)]
        [InlineDataWithRandomSeed(15)]
        [InlineData(5, 1390075326)] // RavenDB-8694
        public void TreeReportContainsInfoAboutStreams(int numberOfStreams, int seed)
        {
            var r = new Random(seed);

            var streamSizes = new Dictionary<string, long>();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("streams-tree");

                for (int streamNumber = 0; streamNumber < numberOfStreams; streamNumber++)
                {
                    var bytes = new byte[r.Next(0, 2 * 1024 * 1024)];

                    r.NextBytes(bytes);

                    var name = $"streams/{streamNumber}";
                    tree.AddStream(name, new MemoryStream(bytes));

                    streamSizes[name] = bytes.Length;
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);

                var treeReport = report.Trees.Find(x => x.Name == "streams-tree");

                Assert.Equal(numberOfStreams, treeReport.Streams.Streams.Count);

                long total = 0;

                foreach (var stream in treeReport.Streams.Streams)
                {
                    Assert.Equal(streamSizes[stream.Name], stream.Length);

                    total += stream.AllocatedSpaceInBytes;
                }

                Assert.Equal(total, treeReport.Streams.AllocatedSpaceInBytes);
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
            var minOverflowSize = tx.LowLevelTransaction.DataPager.NodeMaxSize - Constants.Tree.NodeHeaderSize + 1;
            var entriesAdded = new List<string>();
            var overflowsAdded = 0;

            for (int j = 0; j < treeNumber; j++)
            {
                var overflowSize = r.Next(minOverflowSize, 10000);
                string key = "overflow_" + j;
                tree.Add(key, new MemoryStream(new byte[overflowSize]));

                entriesAdded.Add(key);
                overflowsAdded += VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(overflowSize);
            }

            return new OverflowsAddResult
            {
                AddedEntries = entriesAdded,
                NumberOfOverflowPages = overflowsAdded
            };
        }
    }
}

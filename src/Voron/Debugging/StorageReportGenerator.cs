// -----------------------------------------------------------------------
//  <copyright file="StorageReportGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;

namespace Voron.Debugging
{
    public class ReportInput
    {
        public long NumberOfAllocatedPages;
        public long NumberOfFreePages;
        public long NextPageNumber;
        public List<Tree> Trees;
        public List<FixedSizeTree> FixedSizeTrees;
        public List<JournalFile> Journals;
        public List<Table> Tables;

        public bool IsLightReport { get; set; }
    }

    public unsafe class StorageReportGenerator
    {
        private readonly LowLevelTransaction _tx;

        public StorageReportGenerator(LowLevelTransaction tx)
        {
            _tx = tx;
        }

        public StorageReport Generate(ReportInput input)
        {
            var unallocatedPagesAtEndOfFile = input.NumberOfAllocatedPages - input.NextPageNumber;

            var dataFile = new DataFileReport
            {
                AllocatedSpaceInBytes = PagesToBytes(input.NumberOfAllocatedPages),
                UsedSpaceInBytes = PagesToBytes(input.NextPageNumber - input.NumberOfFreePages),
                FreeSpaceInBytes = PagesToBytes(input.NumberOfFreePages + unallocatedPagesAtEndOfFile)
            };

            var trees = new List<TreeReport>();

            foreach (var tree in input.Trees)
            {
                var treeReport = GetReport(tree, input.IsLightReport == false);

                trees.Add(treeReport);
            }

            foreach (var fst in input.FixedSizeTrees)
            {
                var treeReport = GetReport(fst, input.IsLightReport == false);

                trees.Add(treeReport);
            }

            var tables = new List<TableReport>();
            foreach (var table in input.Tables)
            {
                var tableReport = table.GetReport(input.IsLightReport == false);
                tables.Add(tableReport);
            }

            var journals = input.Journals.Select(journal => new JournalReport
            {
                Number = journal.Number,
                AllocatedSpaceInBytes = PagesToBytes(journal.JournalWriter.NumberOfAllocatedPages)
            }).ToList();

            return new StorageReport
            {
                DataFile = dataFile,
                Trees = trees,
                Tables = tables,
                Journals = journals
            };
        }

        public static TreeReport GetReport(FixedSizeTree fst, bool calculateDensity)
        {
            List<double> pageDensities = null;

            if (calculateDensity)
                pageDensities = GetPageDensities(fst);

            var density = pageDensities?.Average() ?? -1;

            var treeReport = new TreeReport
            {
                Type = RootObjectType.FixedSizeTree,
                Name = fst.Name.ToString(),
                BranchPages = -1,
                Depth = fst.Depth,
                NumberOfEntries = fst.NumberOfEntries,
                LeafPages = -1,
                OverflowPages = 0,
                PageCount = fst.PageCount,
                Density = density,
                AllocatedSpaceInBytes = fst.PageCount * fst.Llt.PageSize,
                UsedSpaceInBytes = calculateDensity ? (long)(fst.PageCount * fst.Llt.PageSize * density) : -1,
                MultiValues = null
            };
            return treeReport;
        }

        public static TreeReport GetReport(Tree tree, bool calculateDensity)
        {
            List<double> pageDensities = null;

            if (calculateDensity)
            {
                pageDensities = GetPageDensities(tree);
            }

            MultiValuesReport multiValues = null;

            if (tree.State.Flags == TreeFlags.MultiValueTrees)
            {
                multiValues = CreateMultiValuesReport(tree);
            }

            var density = pageDensities?.Average() ?? -1;

            var treeReport = new TreeReport
            {
                Type = RootObjectType.VariableSizeTree,
                Name = tree.Name.ToString(),
                BranchPages = tree.State.BranchPages,
                Depth = tree.State.Depth,
                NumberOfEntries = tree.State.NumberOfEntries,
                LeafPages = tree.State.LeafPages,
                OverflowPages = tree.State.OverflowPages,
                PageCount = tree.State.PageCount,
                Density = density,
                AllocatedSpaceInBytes = tree.State.PageCount * tree.Llt.PageSize,
                UsedSpaceInBytes = calculateDensity ? (long)(tree.State.PageCount * tree.Llt.PageSize * density) : -1,
                MultiValues = multiValues
            };

            return treeReport;
        }

        private static MultiValuesReport CreateMultiValuesReport(Tree tree)
        {
            var multiValues = new MultiValuesReport();

            using (var multiTreeIterator = tree.Iterate(false))
            {
                if (multiTreeIterator.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var currentNode = multiTreeIterator.Current;

                        switch (currentNode->Flags)
                        {
                            case TreeNodeFlags.MultiValuePageRef:
                                {
                                    var multiValueTreeHeader = (TreeRootHeader*)((byte*)currentNode + currentNode->KeySize + Constants.NodeHeaderSize);

                                    Debug.Assert(multiValueTreeHeader->Flags == TreeFlags.MultiValue);

                                    multiValues.NumberOfEntries += multiValueTreeHeader->NumberOfEntries;
                                    multiValues.BranchPages += multiValueTreeHeader->BranchPages;
                                    multiValues.LeafPages += multiValueTreeHeader->LeafPages;
                                    multiValues.PageCount += multiValueTreeHeader->PageCount;
                                    break;
                                }
                            case TreeNodeFlags.Data:
                                {
                                    var nestedPage = GetNestedMultiValuePage(tree, tree.DirectAccessFromHeader(currentNode), currentNode);

                                    multiValues.NumberOfEntries += nestedPage.NumberOfEntries;
                                    break;
                                }
                            case TreeNodeFlags.PageRef:
                                {
                                    var overFlowPage = tree.GetReadOnlyTreePage(currentNode->PageNumber);
                                    var nestedPage = GetNestedMultiValuePage(tree, overFlowPage.Base + Constants.TreePageHeaderSize, currentNode);

                                    multiValues.NumberOfEntries += nestedPage.NumberOfEntries;
                                    break;
                                }
                            default:
                                VoronUnrecoverableErrorException.Raise(tree.Llt.Environment, "currentNode->FixedTreeFlags has value of " + currentNode->Flags);
                                break;
                        }
                    } while (multiTreeIterator.MoveNext());
                }
            }
            return multiValues;
        }

        public static List<double> GetPageDensities(Tree tree)
        {
            var allPages = tree.AllPages();
            if (allPages.Count == 0)
                return null;

            var densities = new List<double>();
            var pageSize = tree.Llt.DataPager.PageSize;

            for (var i = 0; i < allPages.Count; i++)
            {
                var page = tree.Llt.GetPage(allPages[i]);

                if (page.IsOverflow)
                {
                    var numberOfPages = tree.Llt.DataPager.GetNumberOfOverflowPages(page.OverflowSize);

                    densities.Add(((double)(page.OverflowSize + Constants.TreePageHeaderSize)) / (numberOfPages * pageSize));

                    i += numberOfPages - 1;
                }
                else
                {
                    if ((page.Flags & PageFlags.FixedSizeTreePage) == PageFlags.FixedSizeTreePage)
                    {
                        var fstp = new FixedSizeTreePage(page.Pointer, tree.Llt.PageSize);
                        var sizeUsed = Constants.FixedSizeTreePageHeaderSize +
                            fstp.NumberOfEntries * (fstp.IsLeaf ? fstp.ValueSize : FixedSizeTree.BranchEntrySize);
                        densities.Add((double)sizeUsed / pageSize);
                    }
                    else
                    {
                        densities.Add(((double)new TreePage(page.Pointer, pageSize).SizeUsed) / pageSize);
                    }
                }
            }
            return densities;
        }

        private static List<double> GetPageDensities(FixedSizeTree tree)
        {
            var allPages = tree.AllPages();
            if (allPages.Count == 0)
                return null;

            var densities = new List<double>();
            var pageSize = tree.Llt.DataPager.PageSize;

            foreach (var pageNumber in allPages)
            {
                var page = tree.Llt.GetPage(pageNumber);
                var fstp = new FixedSizeTreePage(page.Pointer, tree.Llt.PageSize);
                var sizeUsed = Constants.FixedSizeTreePageHeaderSize +
                               fstp.NumberOfEntries * (fstp.IsLeaf ? fstp.ValueSize : FixedSizeTree.BranchEntrySize);
                densities.Add((double)sizeUsed / pageSize);
            }
            return densities;
        }

        private static TreePage GetNestedMultiValuePage(Tree tree, byte* nestedPagePtr, TreeNodeHeader* currentNode)
        {
            var nestedPage = new TreePage(nestedPagePtr, (ushort)tree.GetDataSize(currentNode));

            Debug.Assert(nestedPage.PageNumber == -1); // nested page marker
            return nestedPage;
        }

        private long PagesToBytes(long pageCount)
        {
            return pageCount * _tx.Environment.Options.PageSize;
        }
    }
}

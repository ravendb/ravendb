// -----------------------------------------------------------------------
//  <copyright file="StorageReportGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
using Voron.Impl.Scratch;
using Voron.Util.Settings;

namespace Voron.Debugging
{
    public class ReportInput
    {
        public List<JournalFile> Journals;
        public long NumberOfAllocatedPages { get; set; }
        public int NumberOfFreePages { get; set; }
        public long NextPageNumber { get; set; }
        public int CountOfTrees { get; set; }
        public int CountOfTables { get; set; }
        public VoronPathSetting TempPath { get; set; }
        public VoronPathSetting JournalPath { get; set; }
    }

    public class DetailedReportInput
    {
        public long NumberOfAllocatedPages;
        public long NumberOfFreePages;
        public long NextPageNumber;
        public List<Tree> Trees;
        public List<FixedSizeTree> FixedSizeTrees;
        public List<JournalFile> Journals;
        public List<Table> Tables;
        public ScratchBufferPoolInfo ScratchBufferPoolInfo { get; set; }
        public bool IncludeDetails { get; set; }
        public VoronPathSetting TempPath { get; set; }
        public VoronPathSetting JournalPath { get; set; }
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
            var dataFile = GenerateDataFileReport(input.NumberOfAllocatedPages, input.NumberOfFreePages, input.NextPageNumber);

            var journals = GenerateJournalsReport(input.Journals);

            var tempBuffers = GenerateTempBuffersReport(input.TempPath, input.JournalPath);

            return new StorageReport
            {
                DataFile = dataFile,
                Journals = journals,
                TempFiles = tempBuffers,
                CountOfTables = input.CountOfTables,
                CountOfTrees = input.CountOfTrees
            };
        }

        public DetailedStorageReport Generate(DetailedReportInput input)
        {
            var dataFile = GenerateDataFileReport(input.NumberOfAllocatedPages, input.NumberOfFreePages, input.NextPageNumber);

            var trees = new List<TreeReport>();

            foreach (var tree in input.Trees)
            {
                var treeReport = GetReport(tree, input.IncludeDetails);

                trees.Add(treeReport);
            }

            foreach (var fst in input.FixedSizeTrees)
            {
                var treeReport = GetReport(fst, input.IncludeDetails);

                trees.Add(treeReport);
            }

            var tables = new List<TableReport>();
            foreach (var table in input.Tables)
            {
                var tableReport = table.GetReport(input.IncludeDetails);
                tables.Add(tableReport);
            }

            var journals = GenerateJournalsReport(input.Journals);
            var tempBuffers = GenerateTempBuffersReport(input.TempPath, input.JournalPath);

            return new DetailedStorageReport
            {
                DataFile = dataFile,
                Trees = trees,
                Tables = tables,
                Journals = journals,
                PreAllocatedBuffers = GetReport(new NewPageAllocator(_tx, _tx.RootObjects), input.IncludeDetails),
                ScratchBufferPoolInfo = input.ScratchBufferPoolInfo,
                TempBuffers = tempBuffers,
            };
        }

        private DataFileReport GenerateDataFileReport(long numberOfAllocatedPages, long numberOfFreePages, long nextPageNumber)
        {
            var unallocatedPagesAtEndOfFile = numberOfAllocatedPages - (nextPageNumber - 1);

            return new DataFileReport
            {
                AllocatedSpaceInBytes = PagesToBytes(numberOfAllocatedPages),
                UsedSpaceInBytes = PagesToBytes((nextPageNumber - 1) - numberOfFreePages),
                FreeSpaceInBytes = PagesToBytes(numberOfFreePages + unallocatedPagesAtEndOfFile)
            };
        }

        private List<JournalReport> GenerateJournalsReport(List<JournalFile> journals)
        {
            return journals.Select(journal => new JournalReport
            {
                Number = journal.Number,
                AllocatedSpaceInBytes = (long)journal.JournalWriter.NumberOfAllocated4Kb * 4 * Constants.Size.Kilobyte
            }).ToList();
        }

        private List<TempBufferReport> GenerateTempBuffersReport(VoronPathSetting tempPath, VoronPathSetting journalPath)
        {
            var tempFiles = Directory.GetFiles(tempPath.FullPath, "*.buffers").Select(filePath =>
            {
                try
                {
                    var file = new FileInfo(filePath);

                    return new TempBufferReport
                    {
                        Name = file.Name,
                        AllocatedSpaceInBytes = file.Length,
                        Type = TempBufferType.Scratch
                    };
                }
                catch (FileNotFoundException)
                {
                    // could be deleted meanwhile
                    return null;
                }
            }).Where(x => x != null).ToList();

            if (journalPath != null)
            {
                var recyclableJournals = Directory.GetFiles(journalPath.FullPath, $"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}.*").Select(filePath =>
                {
                    try
                    {
                        var file = new FileInfo(filePath);

                        return new TempBufferReport
                        {
                            Name = file.Name,
                            AllocatedSpaceInBytes = file.Length,
                            Type = TempBufferType.RecyclableJournal
                        };
                    }
                    catch (FileNotFoundException)
                    {
                        // could be deleted meanwhile
                        return null;
                    }
                }).ToList();

                tempFiles.AddRange(recyclableJournals);
            }

            return tempFiles;
        }

        public static TreeReport GetReport(FixedSizeTree fst, bool includeDetails)
        {
            List<double> pageDensities = null;
            if (includeDetails)
            {
                pageDensities = GetPageDensities(fst);
            }

            var density = pageDensities?.Average() ?? -1;

            var treeReport = new TreeReport
            {
                Type = fst.Type ?? RootObjectType.FixedSizeTree,
                Name = fst.Name.ToString(),
                BranchPages = -1,
                Depth = fst.Depth,
                NumberOfEntries = fst.NumberOfEntries,
                LeafPages = -1,
                OverflowPages = 0,
                PageCount = fst.PageCount,
                Density = density,
                AllocatedSpaceInBytes = fst.PageCount * Constants.Storage.PageSize,
                UsedSpaceInBytes = includeDetails ? (long)(fst.PageCount * Constants.Storage.PageSize * density) : -1,
                MultiValues = null,
            };
            return treeReport;
        }

        public static TreeReport GetReport(Tree tree, bool includeDetails)
        {
            List<double> pageDensities = null;
            Dictionary<int, int> pageBalance = null;
            if (includeDetails)
            {
                pageDensities = GetPageDensities(tree);
                pageBalance = GatherBalanceDistribution(tree);
            }

            MultiValuesReport multiValues = null;
            StreamsReport streams = null;

            if (tree.State.Flags == TreeFlags.MultiValueTrees)
            {
                multiValues = CreateMultiValuesReport(tree);
            }
            else if (tree.State.Flags == (TreeFlags.FixedSizeTrees | TreeFlags.Streams))
            {
                streams = CreateStreamsReport(tree);
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
                AllocatedSpaceInBytes = tree.State.PageCount * Constants.Storage.PageSize + (streams?.AllocatedSpaceInBytes ?? 0),
                UsedSpaceInBytes = includeDetails ? (long)(tree.State.PageCount * Constants.Storage.PageSize * density) : -1,
                MultiValues = multiValues,
                Streams = streams,
                BalanceHistogram = pageBalance,
            };

            return treeReport;
        }

        private static StreamsReport CreateStreamsReport(Tree tree)
        {
            var streams = new List<StreamDetails>();

            using (var it = tree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return new StreamsReport();

                long totalNumberOfAllocatedPages = 0;
                do
                {
                    var info = *tree.GetStreamInfo(it.CurrentKey, writable: false);

                    long numberOfAllocatedPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(info.TotalSize + info.TagSize + Tree.StreamInfo.SizeOf);

                    var chunksTree = tree.GetStreamChunksTree(it.CurrentKey);

                    if (chunksTree.Type == RootObjectType.FixedSizeTree) // only if large fst, embedded already counted in parent
                        numberOfAllocatedPages += chunksTree.PageCount;

                    var name = info.TagSize == 0 ? it.CurrentKey.ToString() : tree.GetStreamTag(it.CurrentKey);

                    streams.Add(new StreamDetails
                    {
                        Name = name,
                        Length = info.TotalSize,
                        Version = info.Version,
                        NumberOfAllocatedPages = numberOfAllocatedPages,
                        AllocatedSpaceInBytes = numberOfAllocatedPages * Constants.Storage.PageSize,
                        ChunksTree = GetReport(chunksTree, false),
                    });

                    totalNumberOfAllocatedPages += numberOfAllocatedPages;

                } while (it.MoveNext());
                
                return new StreamsReport
                {
                    Streams = streams,
                    NumberOfStreams = tree.State.NumberOfEntries,
                    TotalNumberOfAllocatedPages = totalNumberOfAllocatedPages,
                    AllocatedSpaceInBytes = totalNumberOfAllocatedPages * Constants.Storage.PageSize
                };
            }
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
                                    var multiValueTreeHeader = (TreeRootHeader*)((byte*)currentNode + currentNode->KeySize + Constants.Tree.NodeHeaderSize);

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
                                    var nestedPage = GetNestedMultiValuePage(tree, overFlowPage.Base + Constants.Tree.PageHeaderSize, currentNode);

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

        public static PreAllocatedBuffersReport GetReport(NewPageAllocator preAllocatedBuffers, bool includeDetails)
        {
            var buffersReport = preAllocatedBuffers.GetNumberOfPreAllocatedFreePages();
            var allocationTreeReport = GetReport(preAllocatedBuffers.GetAllocationStorageFst(), includeDetails);

            return new PreAllocatedBuffersReport
            {
                AllocatedSpaceInBytes = (buffersReport.NumberOfFreePages + allocationTreeReport.PageCount) * Constants.Storage.PageSize,
                PreAllocatedBuffersSpaceInBytes = buffersReport.NumberOfFreePages * Constants.Storage.PageSize,
                NumberOfPreAllocatedPages = buffersReport.NumberOfFreePages,
                AllocationTree = allocationTreeReport,
                OriginallyAllocatedSpaceInBytes = (buffersReport.NumberOfOriginallyAllocatedPages + allocationTreeReport.PageCount) * Constants.Storage.PageSize
            };
        }

        public static Dictionary<int, int> GatherBalanceDistribution(Tree tree)
        {
            var histogram = new Dictionary<int, int>();

            var root = tree.GetReadOnlyTreePage(tree.State.RootPageNumber);

            GatherBalanceDistribution(tree, root, histogram, depth: 1);

            return histogram;
        }

        private static void GatherBalanceDistribution(Tree tree, TreePage page, Dictionary<int, int> histogram, int depth)
        {
            if (page.IsLeaf)
            {
                if (!histogram.TryGetValue(depth, out int value))
                    value = 0;

                histogram[depth] = value + 1;
            }
            else
            {
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    var nodeHeader = page.GetNode(i);
                    var pageNum = nodeHeader->PageNumber;

                    GatherBalanceDistribution(tree, tree.GetReadOnlyTreePage(pageNum), histogram, depth + 1);
                }
            }
        }

        public static List<double> GetPageDensities(Tree tree)
        {
            var allPages = tree.AllPages();
            if (allPages.Count == 0)
                return null;

            var densities = new List<double>();
            
            for (var i = 0; i < allPages.Count; i++)
            {
                var page = tree.Llt.GetPage(allPages[i]);

                if (page.IsOverflow)
                {
                    var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);

                    densities.Add(((double)(page.OverflowSize + Constants.Tree.PageHeaderSize)) / (numberOfPages * Constants.Storage.PageSize));

                    i += numberOfPages - 1;
                }
                else
                {
                    if ((page.Flags & PageFlags.FixedSizeTreePage) == PageFlags.FixedSizeTreePage)
                    {
                        var fstp = new FixedSizeTreePage(page.Pointer, -1, Constants.Storage.PageSize);
                        var sizeUsed = Constants.FixedSizeTree.PageHeaderSize +
                            fstp.NumberOfEntries * (fstp.IsLeaf ? fstp.ValueSize + sizeof(long) : FixedSizeTree.BranchEntrySize);
                        densities.Add((double)sizeUsed / Constants.Storage.PageSize);
                    }
                    else
                    {
                        densities.Add(((double)new TreePage(page.Pointer, Constants.Storage.PageSize).SizeUsed) / Constants.Storage.PageSize);
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
           
            foreach (var pageNumber in allPages)
            {
                var page = tree.Llt.GetPage(pageNumber);
                var fstp = new FixedSizeTreePage(page.Pointer, tree.ValueSize + sizeof(long), Constants.Storage.PageSize);
                var sizeUsed = Constants.FixedSizeTree.PageHeaderSize +
                               fstp.NumberOfEntries * (fstp.IsLeaf ? fstp.ValueSize + sizeof(long) : FixedSizeTree.BranchEntrySize);
                densities.Add((double)sizeUsed / Constants.Storage.PageSize);
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
            return pageCount * Constants.Storage.PageSize;
        }
    }
}

// -----------------------------------------------------------------------
//  <copyright file="StorageReportGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Fixed;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
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
    public sealed class ReportInput
    {
        public List<JournalFile> Journals;
        public JournalFile[] FlushedJournals { get; set; }

        public long NumberOfAllocatedPages { get; set; }
        public int NumberOfFreePages { get; set; }
        public long NextPageNumber { get; set; }
        public int CountOfTrees { get; set; }
        public int CountOfTables { get; set; }

        public int CountOfSets { get; set; }

        public int CountOfContainers { get; set; }

        public int CountOfPersistentDictionaries { get; set; }
        public VoronPathSetting TempPath { get; set; }
        public VoronPathSetting JournalPath { get; set; }
    }

    public sealed class DetailedReportInput
    {
        public long NumberOfAllocatedPages;
        public long NumberOfFreePages;
        public long NextPageNumber;
        public List<Tree> Trees;
        public List<FixedSizeTree> FixedSizeTrees;
        public List<JournalFile> Journals;
        public JournalFile[] FlushedJournals { get; set; }
        public List<Table> Tables;
        public Dictionary<Slice, long> Containers;
        public List<PostingList> PostingLists;
        public List<PersistentDictionaryRootHeader> PersistentDictionaries;
        public List<Lookup<Int64LookupKey>> NumericLookups;
        public List<Lookup<CompactTree.CompactKeyLookup>> TextualLookups;
        public bool IncludeDetails { get; set; }
        public VoronPathSetting TempPath { get; set; }
        public VoronPathSetting JournalPath { get; set; }
        public long LastFlushedTransactionId { get; set; }
        public long LastFlushedJournalId { get; set; }
        public long TotalWrittenButUnsyncedBytes { get; set; }
        public Size TotalEncryptionBufferSize { get; set; }
        public InMemoryStorageState InMemoryStorageState { get; set; }
    }

    public sealed unsafe class StorageReportGenerator
    {
        private readonly LowLevelTransaction _tx;
        private StreamDetails _skippedStreamsDetailsEntry;

        public const string SkippedStreamsDetailsName = "Stream details summary info";

        public StorageReportGenerator(LowLevelTransaction tx)
        {
            _tx = tx;
        }

        public StorageReport Generate(ReportInput input)
        {
            var dataFile = GenerateDataFileReport(input.NumberOfAllocatedPages, input.NumberOfFreePages, input.NextPageNumber);

            var journals = GenerateJournalsReport(input.Journals, input.FlushedJournals);

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

        public event Action<PostingList, TreeReport> HandlePostingListDetails;

        public unsafe DetailedStorageReport Generate(DetailedReportInput input)
        {
            var dataFile = GenerateDataFileReport(input.NumberOfAllocatedPages, input.NumberOfFreePages, input.NextPageNumber);

            long streamsAllocatedSpaceInBytes = 0;
            long treesAllocatedSpaceInBytes = 0;
            var trees = new List<TreeReport>();
            foreach (var tree in input.Trees)
            {
                var treeReport = GetReport(tree, input.IncludeDetails);
                trees.Add(treeReport);
                TreeFlags treeFlags = tree.ReadHeader().Flags;
                if (treeFlags.HasFlag(TreeFlags.CompactTrees) ||
                    treeFlags.HasFlag(TreeFlags.Lookups))
                {
                    using var it = tree.Iterate(false);
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            ReadResult readResult = tree.Read(it.CurrentKey);
                            RootObjectType rootObjectType = (RootObjectType)readResult.Reader.ReadByte();
                            switch (rootObjectType)
                            {
                                case RootObjectType.Lookup:
                                    var lookup = tree.LookupFor<Int64LookupKey>(it.CurrentKey);

                                    if (lookup.State.DictionaryId < 0)
                                    {
                                        var nestedReport = GetReport(lookup, input.IncludeDetails);
                                        nestedReport.Name = treeReport.Name + "/" + nestedReport.Name + " (Lookup)";
                                        trees.Add(nestedReport);
                                    }
                                    else
                                    {
                                        tree.Forget(it.CurrentKey);
                                        var textLookup = tree.LookupFor<CompactTree.CompactKeyLookup>(it.CurrentKey);
                                        var nestedReport = GetReport(textLookup, input.IncludeDetails);
                                        string nestedReportName = treeReport.Name + "/" + nestedReport.Name;
                                        nestedReport.Name = nestedReportName +" (CompactTree)";
                                        trees.Add(nestedReport);
                                        trees.Add(GetContainerReport(nestedReportName +" (Entries)", textLookup.State.TermsContainerId, input.IncludeDetails));
                                    }
                                    break;
                                case RootObjectType.EmbeddedFixedSizeTree:
                                    continue; // already accounted for
                                case RootObjectType.FixedSizeTree:
                                    var header = (FixedSizeTreeHeader.Large*)readResult.Reader.Base;
                                    var set = tree.FixedTreeFor(it.CurrentKey, (byte)header->ValueSize);
                                    var nestedSetReport = GetReport(set, input.IncludeDetails);
                                    nestedSetReport.Name = treeReport.Name + "/" + nestedSetReport.Name +", FixedSizeTree";
                                    trees.Add(nestedSetReport);
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException(rootObjectType.ToString());

                            }

                        } while (it.MoveNext());
                    }
                }

                if (input.IncludeDetails)
                    continue;

                if (treeReport.Streams == null)
                    treesAllocatedSpaceInBytes += treeReport.AllocatedSpaceInBytes;
                else
                    streamsAllocatedSpaceInBytes += treeReport.Streams.AllocatedSpaceInBytes;
            }

            foreach (PostingList postingList in input.PostingLists)
            {
                var report = GetReport(postingList, input.IncludeDetails);
                HandlePostingListDetails?.Invoke(postingList, report);
                trees.Add(report);
            }

            foreach (var (name, page) in input.Containers)
            {
                trees.Add(GetContainerReport(name.ToString(), page, input.IncludeDetails));
            }

            foreach (PersistentDictionaryRootHeader dic in input.PersistentDictionaries)
            {
                // cannot use GetPageHeaderForDebug since the header is at the data pointer
                Page page = _tx.GetPage(dic.PageNumber);
                var header = (PersistentDictionaryHeader*)page.DataPointer;

                int usedPages = Paging.GetNumberOfOverflowPages(page.OverflowSize);
                trees.Add(new TreeReport
                {
                    Name = "Dictionary #" + dic.PageNumber,
                    AllocatedSpaceInBytes = PagesToBytes(usedPages),
                    UsedSpaceInBytes = header->TableSize,
                    OverflowPages = usedPages,
                    PageCount = usedPages,
                    NumberOfEntries = 1,
                });
            }

            foreach (var nl in input.NumericLookups)
            {
                trees.Add(GetReport(nl, input.IncludeDetails));
            }

            foreach (var tl in input.TextualLookups)
            {
                trees.Add(GetReport(tl, input.IncludeDetails));
            }

            foreach (var fst in input.FixedSizeTrees)
            {
                var treeReport = GetReport(fst, input.IncludeDetails);
                trees.Add(treeReport);

                treesAllocatedSpaceInBytes += treeReport.AllocatedSpaceInBytes;
            }

            long _tablesAllocatedSpaceInBytes = 0;
            var tables = new List<TableReport>();
            foreach (var table in input.Tables)
            {
                var tableReport = table.GetReport(input.IncludeDetails, this);
                tables.Add(tableReport);

                _tablesAllocatedSpaceInBytes += tableReport.AllocatedSpaceInBytes;
            }

            var journals = new JournalsReport
            {
                Journals = GenerateJournalsReport(input.Journals, input.FlushedJournals),
                LastFlushedJournal = input.LastFlushedJournalId,
                LastFlushedTransaction = input.LastFlushedTransactionId,
                TotalWrittenButUnsyncedBytes = input.TotalWrittenButUnsyncedBytes
            };

            var tempBuffers = GenerateTempBuffersReport(input.TempPath, input.JournalPath);
            var preAllocatedBuffers = GetReport(new NewPageAllocator(_tx, _tx.RootObjects), input.IncludeDetails);

            if (input.IncludeDetails == false && _skippedStreamsDetailsEntry != null)
            {
                // we don't have the actual trees' streams size at this point
                // so we calculate the original size as if we read the streams by:
                // [DataFile allocated space] - [DataFile free space] - [Tables allocated space] - [FixedTrees allocated space] - [pre allocated buffers space] 

                var treesCalculatedSpaceInBytes = dataFile.UsedSpaceInBytes - _tablesAllocatedSpaceInBytes - preAllocatedBuffers.AllocatedSpaceInBytes - treesAllocatedSpaceInBytes;

                foreach (var tree in trees)
                {
                    if (tree.Streams?.Streams != null && tree.Streams.Streams.Count > 0 && tree.Streams.Streams[0].Name == SkippedStreamsDetailsName)
                    {
                        _skippedStreamsDetailsEntry.AllocatedSpaceInBytes = treesCalculatedSpaceInBytes;
                        _skippedStreamsDetailsEntry.Length = treesCalculatedSpaceInBytes;
                        tree.AllocatedSpaceInBytes = treesCalculatedSpaceInBytes - streamsAllocatedSpaceInBytes;
                        break;
                    }
                }
            }

            return new DetailedStorageReport
            {
                InMemoryState = input.InMemoryStorageState,
                DataFile = dataFile,
                Trees = trees,
                Tables = tables,
                Journals = journals,
                PreAllocatedBuffers = preAllocatedBuffers,
                TempBuffers = tempBuffers,
                TotalEncryptionBufferSize = input.TotalEncryptionBufferSize.ToString()
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

        private List<JournalReport> GenerateJournalsReport(List<JournalFile> journals, JournalFile[] flushedJournals)
        {
            var journalReports = journals.Select(journal =>
            {
                var journalWriter = journal.JournalWriter;

                if (journalWriter == null)
                    return null;

                return new JournalReport
                {
                    Flushed = false,
                    Number = journal.Number,
                    AllocatedSpaceInBytes = (long)journalWriter.NumberOfAllocated4Kb * 4 * Constants.Size.Kilobyte,
                    Available4Kbs = journal.GetAvailable4Kbs(_tx.CurrentStateRecord),
                    LastTransaction = journal.LastTransactionId,
                };
            });
            var flushedJournalReports = flushedJournals.Select(journal =>
            {
                var journalWriter = journal.JournalWriter;

                if (journalWriter == null)
                    return null;

                return new JournalReport
                {
                    Flushed = true,
                    Number = journal.Number,
                    AllocatedSpaceInBytes = (long)journalWriter.NumberOfAllocated4Kb * 4 * Constants.Size.Kilobyte,
                    Available4Kbs = journal.GetAvailable4Kbs(_tx.CurrentStateRecord),
                    LastTransaction = journal.LastTransactionId,
                };
            });
            return journalReports.Concat(flushedJournalReports).Where(x => x != null).ToList();
        }

        public static List<TempBufferReport> GenerateTempBuffersReport(VoronPathSetting tempPath, VoronPathSetting journalPath)
        {
            var tempFiles = GetFiles(tempPath.FullPath, $"*{StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.BuffersFileExtension}").Select(filePath =>
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
                var recyclableJournals = GetFiles(journalPath.FullPath, $"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}.*").Select(filePath =>
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
                }).Where(x => x != null).ToList();

                tempFiles.AddRange(recyclableJournals);
            }

            return tempFiles;

            IEnumerable<string> GetFiles(string path, string searchPattern)
            {
                try
                {
                    return Directory.GetFiles(path, searchPattern);
                }
                catch (DirectoryNotFoundException)
                {
                    return Enumerable.Empty<string>();
                }
            }
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
                AllocatedSpaceInBytes = PagesToBytes(fst.PageCount),
                UsedSpaceInBytes = includeDetails ? (long)(PagesToBytes(fst.PageCount) * density) : -1,
                MultiValues = null,
            };
            return treeReport;
        }

        public TreeReport GetReport(PostingList postingList, bool includeDetails)
        {
            List<double> pageDensities = null;
            if (includeDetails)
            {
                pageDensities = GetPageDensities(postingList);
            }
            int pageCount = postingList.State.BranchPages + postingList.State.LeafPages;
            double density = pageDensities?.Average() ?? -1;
            var treeReport = new TreeReport
            {
                Type = RootObjectType.Set,
                Name = postingList.Name.ToString(),
                BranchPages = postingList.State.BranchPages,
                Depth = postingList.State.Depth,
                NumberOfEntries = postingList.State.NumberOfEntries,
                LeafPages = postingList.State.LeafPages,
                PageCount = pageCount,
                Density = density,
                AllocatedSpaceInBytes = PagesToBytes(pageCount),
                UsedSpaceInBytes = includeDetails ? (long)(PagesToBytes(pageCount) * density) : -1,
            };

            return treeReport;
        }

        public TreeReport GetReport(Lookup<Int64LookupKey> lookup, bool includeDetails)
        {
            List<double> pageDensities = null;
            if (includeDetails)
            {
                pageDensities = GetLookupPageDensities(lookup.Llt, lookup.AllPages());
            }

            long pageCount = lookup.State.BranchPages + lookup.State.LeafPages;

            double density = pageDensities?.Average() ?? -1;

            var treeReport = new TreeReport
            {
                Type = RootObjectType.Lookup,
                Name = lookup.Name.ToString(),
                BranchPages = lookup.State.BranchPages,
                NumberOfEntries = lookup.State.NumberOfEntries,
                LeafPages = lookup.State.LeafPages,
                PageCount = pageCount,
                OverflowPages = 0,
                Density = density,
                AllocatedSpaceInBytes = PagesToBytes(pageCount),
                UsedSpaceInBytes = includeDetails ? (long)(PagesToBytes(pageCount) * density) : -1,
            };

            return treeReport;
        }

        public TreeReport GetReport(Lookup<CompactTree.CompactKeyLookup> lookup, bool includeDetails)
        {
            List<double> pageDensities = null;
            if (includeDetails)
            {
                pageDensities = GetLookupPageDensities(lookup.Llt, lookup.AllPages());
            }
            
            // CompactTree also has a ContainerId, but that is accounted for _separately_, using the EntriesTerms
            long pageCount = lookup.State.BranchPages + lookup.State.LeafPages;

            double density = pageDensities?.Average() ?? -1;

            var treeReport = new TreeReport
            {
                Type = RootObjectType.Set,
                Name = lookup.Name.ToString(),
                BranchPages = lookup.State.BranchPages,
                NumberOfEntries = lookup.State.NumberOfEntries,
                LeafPages = lookup.State.LeafPages,
                PageCount = pageCount,
                OverflowPages = 0,
                Density = density,
                AllocatedSpaceInBytes = PagesToBytes(pageCount),
                UsedSpaceInBytes = includeDetails ? (long)(PagesToBytes(pageCount) * density) : -1,
            };

            return treeReport;
        }

        public TreeReport GetContainerReport(string name, long page, bool includeDetails)
        {
            List<double> pageDensities = null;

            if (includeDetails)
            {
                pageDensities = new();
                var it = Container.GetAllPagesIterator(_tx, page);
                while (it.MoveNext(out var pageNum))
                {
                    // cannot use GetPageHeaderForDebug since we are reading not just from the header
                    Page cur = _tx.GetPage(pageNum);
                    if (cur.IsOverflow)
                    {
                        int numberOfOverflowPages = Paging.GetNumberOfOverflowPages(cur.OverflowSize);
                        pageDensities.Add((double)cur.OverflowSize / (numberOfOverflowPages * Constants.Storage.PageSize));
                    }
                    else
                    {
                        var container = new Container(cur);
                        pageDensities.Add((double)container.SpaceUsed() / Constants.Storage.PageSize);
                    }
                }
            }
            
            var (allPages, freePages) = Container.GetPagesFor(_tx, page);
            
            // cannot use GetPageHeaderForDebug since we are reading not just from the header
            var root = new Container(_tx.GetPage(page));
            double density = pageDensities?.Average() ?? -1;
            long totalPages = allPages.State.NumberOfEntries + root.Header.NumberOfOverflowPages + freePages.State.PageCount + allPages.State.PageCount;
            var treeReport = new TreeReport
            {
                Type = RootObjectType.Set,
                Name = name.ToString(),
                NumberOfEntries = root.GetNumberOfEntries(),
                LeafPages = allPages.State.NumberOfEntries + allPages.State.LeafPages + freePages.State.LeafPages,
                OverflowPages = root.Header.NumberOfOverflowPages,
                BranchPages = +freePages.State.BranchPages + allPages.State.BranchPages,
                PageCount = totalPages ,
                Density = density,
                AllocatedSpaceInBytes = PagesToBytes(totalPages),
                UsedSpaceInBytes = includeDetails ? (long)(PagesToBytes(totalPages) * density) : -1,
            };

            return treeReport;
        }

        public TreeReport GetReport(Tree tree, bool includeDetails)
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

            ref readonly var state = ref tree.ReadHeader();

            if (state.Flags == TreeFlags.MultiValueTrees)
            {
                multiValues = CreateMultiValuesReport(tree);
            }
            else if (state.Flags == (TreeFlags.FixedSizeTrees | TreeFlags.Streams))
            {
                streams = CreateStreamsReport(tree, includeDetails);
            }

            var density = pageDensities?.Average() ?? -1;

            var treeReport = new TreeReport
            {
                Type = RootObjectType.VariableSizeTree,
                Name = tree.Name.ToString(),
                BranchPages = state.BranchPages,
                Depth = state.Depth,
                NumberOfEntries = state.NumberOfEntries,
                LeafPages = state.LeafPages,
                OverflowPages = state.OverflowPages,
                PageCount = state.PageCount,
                Density = density,
                AllocatedSpaceInBytes = PagesToBytes(state.PageCount) + (streams?.AllocatedSpaceInBytes ?? 0),
                UsedSpaceInBytes = includeDetails ? (long)(PagesToBytes(state.PageCount) * density) : -1,
                MultiValues = multiValues,
                Streams = streams,
                BalanceHistogram = pageBalance,
            };

            return treeReport;
        }

        private StreamsReport CreateStreamsReport(Tree tree, bool includeDetails = false)
        {
            if (includeDetails == false)
            {
                // there are cases we don't need/want to pull the entire data for the report
                // so we create a report that includes the metadata info
                // without reading the actual data 

                // we may have multiple such entries, in theory. We intentionally 
                // override it and will use the last one. RavenDB at the time of writing
                // this used just one

                _skippedStreamsDetailsEntry = new StreamDetails
                {
                    Name = SkippedStreamsDetailsName,
                    AllocatedSpaceInBytes = 0,
                    ChunksTree = new TreeReport(),
                    Length = 0,
                    NumberOfAllocatedPages = 0,
                    Version = 1
                };

                return new StreamsReport
                {
                    AllocatedSpaceInBytes = -1,
                    NumberOfStreams = tree.ReadHeader().NumberOfEntries,
                    Streams = [_skippedStreamsDetailsEntry],
                    TotalNumberOfAllocatedPages = -1
                };
            }

            var streams = new List<StreamDetails>();

            using (var it = tree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return new StreamsReport();

                long totalNumberOfAllocatedPages = 0;
                do
                {
                    var info = tree.GetStreamInfoForReporting(it.CurrentKey, out var tag);
                    if (info.HasValue == false)
                        continue;

                    long numberOfAllocatedPages = Paging.GetNumberOfOverflowPages(info.Value.TotalSize + info.Value.TagSize + Tree.StreamInfo.SizeOf);

                    var chunksTree = tree.GetStreamChunksTree(it.CurrentKey);

                    if (chunksTree.Type == RootObjectType.FixedSizeTree) // only if large fst, embedded already counted in parent
                        numberOfAllocatedPages += chunksTree.PageCount;

                    var name = tag ?? it.CurrentKey.ToString();

                    streams.Add(new StreamDetails
                    {
                        Name = name,
                        Length = info.Value.TotalSize,
                        Version = info.Value.Version,
                        NumberOfAllocatedPages = numberOfAllocatedPages,
                        AllocatedSpaceInBytes = PagesToBytes(numberOfAllocatedPages),
                        ChunksTree = GetReport(chunksTree, false),
                    });

                    totalNumberOfAllocatedPages += numberOfAllocatedPages;

                } while (it.MoveNext());

                return new StreamsReport
                {
                    Streams = streams,
                    NumberOfStreams = tree.ReadHeader().NumberOfEntries,
                    TotalNumberOfAllocatedPages = totalNumberOfAllocatedPages,
                    AllocatedSpaceInBytes = PagesToBytes(totalNumberOfAllocatedPages)
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
                                VoronUnrecoverableErrorException.Raise(tree.Llt, "currentNode->FixedTreeFlags has value of " + currentNode->Flags);
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
                AllocatedSpaceInBytes = PagesToBytes(buffersReport.NumberOfFreePages + allocationTreeReport.PageCount),
                PreAllocatedBuffersSpaceInBytes = PagesToBytes(buffersReport.NumberOfFreePages),
                NumberOfPreAllocatedPages = buffersReport.NumberOfFreePages,
                AllocationTree = allocationTreeReport,
                OriginallyAllocatedSpaceInBytes = PagesToBytes(buffersReport.NumberOfOriginallyAllocatedPages + allocationTreeReport.PageCount)
            };
        }

        public static Dictionary<int, int> GatherBalanceDistribution(Tree tree)
        {
            var histogram = new Dictionary<int, int>();

            var root = tree.GetReadOnlyTreePage(tree.ReadHeader().RootPageNumber);

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
                // we don't need the entire page contents in order to calculate the page density, just the header
                var pageHeaderUnion = tree.Llt.GetPageHeaderForDebug<PageHeaderUnion>(allPages[i]);

                if ((pageHeaderUnion.PageHeader.Flags & PageFlags.Overflow) == PageFlags.Overflow)
                {
                    var numberOfPages = Paging.GetNumberOfOverflowPages(pageHeaderUnion.PageHeader.OverflowSize);

                    densities.Add(((double)(pageHeaderUnion.PageHeader.OverflowSize + Constants.Tree.PageHeaderSize)) / PagesToBytes(numberOfPages));

                    i += numberOfPages - 1;
                }
                else
                {
                    if ((pageHeaderUnion.PageHeader.Flags & PageFlags.FixedSizeTreePage) == PageFlags.FixedSizeTreePage)
                    {
                        var isLeaf = (pageHeaderUnion.FixedSizeTreePageHeader.TreeFlags & FixedSizeTreePageFlags.Leaf) == FixedSizeTreePageFlags.Leaf;
                        var sizeUsed = Constants.FixedSizeTree.PageHeaderSize +
                                       pageHeaderUnion.FixedSizeTreePageHeader.NumberOfEntries * (isLeaf ? pageHeaderUnion.FixedSizeTreePageHeader.ValueSize + sizeof(long) : FixedSizeTree.BranchEntrySize);
                        densities.Add((double)sizeUsed / Constants.Storage.PageSize);
                    }
                    else
                    {
                        var sizeLeft = pageHeaderUnion.TreePageHeader.Upper - pageHeaderUnion.TreePageHeader.Lower;
                        densities.Add(((double)(Constants.Storage.PageSize - sizeLeft) / Constants.Storage.PageSize));
                    }
                }
            }

            return densities;
        }

        public static List<double> GetPageDensities(PostingList postingList)
        {
            var allPages = postingList.AllPages();
            if (allPages.Count == 0)
                return null;

            var densities = new List<double>();

            foreach (var p in allPages)
            {
                // cannot use GetPageHeaderForDebug since we are reading not just from the header
                var page = postingList.Llt.GetPage(p);
                var state = new PostingListCursorState { Page = page };
                if (state.IsLeaf)
                {
                    densities.Add((double)new PostingListLeafPage(page).SpaceUsed / Constants.Storage.PageSize);
                }
                else
                {
                    densities.Add((double)new PostingListBranchPage(page).SpaceUsed / Constants.Storage.PageSize);
                }
            }

            return densities;
        }

        public static List<double> GetLookupPageDensities(LowLevelTransaction llt, List<long> allPages)
        {
            if (allPages.Count == 0)
                return null;

            var densities = new List<double>();
            foreach (var p in allPages)
            {
                var lookupPageHeader = llt.GetPageHeaderForDebug<LookupPageHeader>(p);
                densities.Add((double)lookupPageHeader.FreeSpace / Constants.Storage.PageSize);
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
                var fstph = tree.Llt.GetPageHeaderForDebug<FixedSizeTreePageHeader>(pageNumber);
                var isLeaf = (fstph.TreeFlags & FixedSizeTreePageFlags.Leaf) == FixedSizeTreePageFlags.Leaf;
                var sizeUsed = Constants.FixedSizeTree.PageHeaderSize +
                               fstph.NumberOfEntries * (isLeaf ? fstph.ValueSize + sizeof(long) : FixedSizeTree.BranchEntrySize);
                densities.Add((double)sizeUsed / Constants.Storage.PageSize);
            }

            return densities;
        }

        private static TreePage GetNestedMultiValuePage(Tree tree, byte* nestedPagePtr, TreeNodeHeader* currentNode)
        {
            var nestedPage = new TreePage(nestedPagePtr, (ushort)tree.GetDataSize(currentNode));

            //Debug.Assert(nestedPage.PageNumber == -1); // nested page marker
            return nestedPage;
        }

        public static long PagesToBytes(long pageCount)
        {
            return pageCount * Constants.Storage.PageSize;
        }
    }
}

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl.Paging;
using static Voron.Data.BTrees.Tree;

namespace Voron.Recovery
{
    public unsafe class Recovery : IDisposable
    {
        public enum RecoveryStatus
        {
            Success,
            CancellationRequested
        }

        private const int SizeOfMacInBytes = 16;

        private const string EncryptedDatabaseWithoutMasterKeyErrorMessage =
            "this is a strong indication that you're recovering an encrypted database and didn't" +
            " provide the encryption key using the  '--MasterKey=<KEY>' command line flag";

        private const string LogFileName = "recovery.log";
        private const int MaxNumberOfInvalidChecksumWithNoneZeroMac = 128;
        private readonly List<(IntPtr Ptr, int Size)> _attachmentChunks = new List<(IntPtr Ptr, int Size)>();
        private readonly VoronRecoveryConfiguration _config;
        private readonly bool _copyOnWrite;
        private readonly string _datafile;
        private readonly int _initialContextLongLivedSize;
        private readonly int _initialContextSize;
        private readonly byte[] _masterKey;

        private readonly string _output;
        private readonly int _pageSize;
        private readonly Dictionary<string, long> _previouslyWrittenDocs;
        private readonly int _progressIntervalInSec;
        private readonly byte[] _streamHashResult = new byte[(int)Sodium.crypto_generichash_bytes()];

        private readonly byte[] _streamHashState = new byte[(int)Sodium.crypto_generichash_statebytes()];
        //private readonly SortedSet<(string name, string docId)> _uniqueCountersDiscovered = new SortedSet<(string name, string docId)>(new ByDocIdAndCounterName());

        private bool _cancellationRequested;
        private int _invalidChecksumWithNoneZeroMac;
        // private string _lastRecoveredDocumentKey = "No documents recovered yet";
        private readonly Logger _logger;

        private readonly Size _maxTransactionSize = new Size(64, SizeUnit.Megabytes);

        private long _numberOfFaultedPages;
        private StorageEnvironmentOptions _option;
        private readonly DocumentDatabase _recoveredDatabase;
        private bool _shouldIgnoreInvalidPagesInARaw;
        private long _numberOfDocumentsRetrieved;
        private long _numberOfAttachmentsRetrieved;
        private long _numberOfCountersRetrieved;
        private long _numberOfRevisionsRetrieved;
        private long _numberOfConflictsRetrieved;
        private string _lastRecoveredKey = "(Nothing discovered yet)";

        public Recovery(VoronRecoveryConfiguration config)
        {
            _datafile = config.PathToDataFile;
            _output = config.LoggingOutputPath;
            _pageSize = config.PageSizeInKB * Constants.Size.Kilobyte;
            _initialContextSize = config.InitialContextSizeInMB * Constants.Size.Megabyte;
            _initialContextLongLivedSize = config.InitialContextLongLivedSizeInKB * Constants.Size.Kilobyte;

            _masterKey = config.MasterKey;

            // by default CopyOnWriteMode will be true
            _copyOnWrite = !config.DisableCopyOnWriteMode;
            _config = config;
            _option = CreateOptions();

            _progressIntervalInSec = config.ProgressIntervalInSec;
            _previouslyWrittenDocs = new Dictionary<string, long>();
            if (config.LoggingMode != LogMode.None)
                LoggingSource.Instance.SetupLogMode(config.LoggingMode, Path.Combine(Path.GetDirectoryName(_output), LogFileName), TimeSpan.FromDays(3), long.MaxValue,
                    false);
            _logger = LoggingSource.Instance.GetLogger<Recovery>("Voron Recovery");
            _shouldIgnoreInvalidPagesInARaw = config.IgnoreInvalidPagesInARow;
            _recoveredDatabase = config.RecoveredDatabase;
        }

        private AbstractPager Pager => _option.DataPager;

        public bool IsEncrypted => _masterKey != null;

        public void Dispose()
        {
            _option?.Dispose();
        }

        private StorageEnvironmentOptions CreateOptions()
        {
            var result = StorageEnvironmentOptions.ForPath(_config.DataFileDirectory, null, null, null, null);
            result.CopyOnWriteMode = _copyOnWrite;
            result.ManualFlushing = true;
            result.ManualSyncing = true;
            result.IgnoreInvalidJournalErrors = _config.IgnoreInvalidJournalErrors;
            result.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = _config.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;
            result.MasterKey = _masterKey;
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetFilePosition(long offset, byte* position)
        {
            return (long)position - offset;
        }

        public RecoveryStatus Execute(TextWriter writer, CancellationToken ct)
        {
            void PrintRecoveryProgress(long startOffset, byte* mem, byte* eof, DateTime now)
            {
                var currPos = GetFilePosition(startOffset, mem);
                var eofPos = GetFilePosition(startOffset, eof);
                writer.WriteLine(
                    $"{now:hh:MM:ss}: Recovering page at position {currPos:#,#;;0}/{eofPos:#,#;;0} ({(double)currPos / eofPos:p}) - Last recovered {_lastRecoveredKey}");
            }

            StorageEnvironment se = null;
            TempPagerTransaction tx = null;
            try
            {
                if (IsEncrypted)
                    //We need a tx for the encryption pager and we can't dispose of it while reading the page.
                    tx = new TempPagerTransaction();
                var sw = new Stopwatch();
                sw.Start();
                byte* mem = null;
                if (_copyOnWrite)
                {
                    writer.WriteLine("Recovering journal files, this may take a while...");

                    bool optionOwnsPagers = _option.OwnsPagers;
                    try
                    {
                        _option.OwnsPagers = false;

                        while (true)
                            try
                            {
                                se = new StorageEnvironment(_option);
                                break;
                            }
                            catch (IncreasingDataFileInCopyOnWriteModeException ex)
                            {
                                _option.Dispose();

                                using (var file = File.Open(ex.DataFilePath, FileMode.Open))
                                {
                                    file.SetLength(ex.RequestedSize);
                                }

                                _option = CreateOptions();
                            }
                            catch (OutOfMemoryException e)
                            {
                                if (e.InnerException is Win32Exception)
                                    throw;
                            }

                        //for encrypted database the pointer points to the buffer and this is not what we want.
                        mem = se.Options.DataPager.PagerState.MapBase;
                        writer.WriteLine(
                            $"Journal recovery has completed successfully within {sw.Elapsed.TotalSeconds:N1} seconds");
                    }
                    catch (Exception e)
                    {
                        se?.Dispose();

                        if (e is OutOfMemoryException && e.InnerException is Win32Exception)
                        {
                            e.Data["ReturnCode"] = 0xDEAD;
                            writer.WriteLine($"{e.InnerException.Message}. {e.Message}.");
                            writer.WriteLine();
                            writer.WriteLine("Journal recovery failed. To continue, please backup your files and run again with --DisableCopyOnWriteMode flag.");
                            writer.WriteLine("Please note that this is unsafe operation and we highly recommend to backup you files.");

                            throw;
                        }

                        writer.WriteLine("Journal recovery failed, don't worry we will continue with data recovery.");
                        writer.WriteLine("The reason for the Journal recovery failure was:");
                        writer.WriteLine(e);
                    }
                    finally
                    {
                        _option.OwnsPagers = optionOwnsPagers;
                    }
                }

                if (mem == null)
                {
                    //for encrypted database the pointer points to the buffer and this is not what we want.
                    if (se == null /*journal recovery failed or copy on write is set to false*/)
                        mem = _option.DataPager.PagerState.MapBase;
                    else
                        mem = se.Options.DataPager.PagerState.MapBase;
                }

                long startOffset = (long)mem;
                var fi = new FileInfo(_datafile);
                var fileSize = fi.Length;
                //making sure eof is page aligned
                var eof = mem + fileSize / _pageSize * _pageSize;

                DateTime lastProgressReport = DateTime.MinValue;

                if (Directory.Exists(Path.GetDirectoryName(_output)) == false)
                    Directory.CreateDirectory(Path.GetDirectoryName(_output));

                using (var context = new JsonOperationContext(_initialContextSize, _initialContextLongLivedSize, SharedMultipleUseFlag.None))
                using (var recoveredTool = RecoveredDatabaseCreator.RecoveredDbTools(_recoveredDatabase, DateTime.UtcNow.Ticks.ToString(), _logger))
                {
                    while (mem < eof)
                        try
                        {
                            var page = DecryptPageIfNeeded(mem, startOffset, ref tx, true);

                            if (ct.IsCancellationRequested)
                            {
                                if (_logger.IsOperationsEnabled)
                                    _logger.Operations($"Cancellation requested while recovery was in position {GetFilePosition(startOffset, mem)}");
                                _cancellationRequested = true;
                                break;
                            }

                            var now = DateTime.UtcNow;
                            if ((now - lastProgressReport).TotalSeconds >= _progressIntervalInSec)
                            {
                                if (lastProgressReport != DateTime.MinValue) writer.WriteLine("Press 'q' to quit the recovery process");

                                lastProgressReport = now;
                                PrintRecoveryProgress(startOffset, mem, eof, now);
                            }

                            var pageHeader = (PageHeader*)page;

                            //this page is not raw data section move on
                            if (pageHeader->Flags.HasFlag(PageFlags.RawData) == false && pageHeader->Flags.HasFlag(PageFlags.Stream) == false)
                            {
                                mem += _pageSize;
                                continue;
                            }

                            if (pageHeader->Flags.HasFlag(PageFlags.Single) &&
                                pageHeader->Flags.HasFlag(PageFlags.Overflow))
                            {
                                var message =
                                    $"page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)}) has both Overflow and Single flag turned";
                                mem = PrintErrorAndAdvanceMem(message, mem);
                                continue;
                            }

                            //overflow page
                            if (pageHeader->Flags.HasFlag(PageFlags.Overflow))
                            {
                                if (ValidateOverflowPage(pageHeader, eof, startOffset, ref mem) == false)
                                    continue;

                                var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);

                                if (pageHeader->Flags.HasFlag(PageFlags.Stream))
                                {
                                    var streamPageHeader = (StreamPageHeader*)pageHeader;
                                    //Skipping stream chunks that are not first, this is not a faulty page so we don't report error
                                    if (streamPageHeader->StreamPageFlags.HasFlag(StreamPageFlags.First) == false)
                                    {
                                        mem += numberOfPages * _pageSize;
                                        continue;
                                    }

                                    fixed (byte* hashStatePtr = _streamHashState)
                                    fixed (byte* hashResultPtr = _streamHashResult)
                                    {
                                        long totalSize = 0;
                                        _attachmentChunks.Clear();
                                        int rc = Sodium.crypto_generichash_init(hashStatePtr, null, UIntPtr.Zero, (UIntPtr)_streamHashResult.Length);
                                        if (rc != 0)
                                        {
                                            if (_logger.IsOperationsEnabled)
                                                _logger.Operations(
                                                    $"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to initialize Sodium for hash computation will skip this page.");
                                            mem += numberOfPages * _pageSize;
                                            continue;
                                        }

                                        // write document header, including size
                                        PageHeader* nextPage = pageHeader;

                                        bool valid = true;
                                        string tag = null;

                                        while (true) // has next
                                        {
                                            streamPageHeader = (StreamPageHeader*)nextPage;
                                            //this is the last page and it contains only stream info + maybe the stream tag
                                            if (streamPageHeader->ChunkSize == 0)
                                            {
                                                ExtractTagFromLastPage(nextPage, streamPageHeader, ref tag);
                                                break;
                                            }

                                            totalSize += streamPageHeader->ChunkSize;
                                            var dataStart = (byte*)nextPage + PageHeader.SizeOf;
                                            _attachmentChunks.Add(((IntPtr)dataStart, (int)streamPageHeader->ChunkSize));
                                            rc = Sodium.crypto_generichash_update(hashStatePtr, dataStart, (ulong)streamPageHeader->ChunkSize);
                                            if (rc != 0)
                                            {
                                                if (_logger.IsOperationsEnabled)
                                                    _logger.Operations(
                                                        $"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to compute chunk hash, will skip it.");
                                                valid = false;
                                                break;
                                            }

                                            if (streamPageHeader->StreamNextPageNumber == 0)
                                            {
                                                ExtractTagFromLastPage(nextPage, streamPageHeader, ref tag);
                                                break;
                                            }

                                            var nextStreamHeader = (byte*)(streamPageHeader->StreamNextPageNumber * _pageSize) + startOffset;
                                            nextPage = (PageHeader*)DecryptPageIfNeeded(nextStreamHeader, startOffset, ref tx);

                                            //This is the case that the next page isn't a stream page
                                            if (nextPage->Flags.HasFlag(PageFlags.Stream) == false || nextPage->Flags.HasFlag(PageFlags.Overflow) == false)
                                            {
                                                valid = false;
                                                if (_logger.IsOperationsEnabled)
                                                    _logger.Operations(
                                                        $"page #{nextPage->PageNumber} (offset={(long)nextPage}) was suppose to be a stream chunk but isn't marked as Overflow | Stream");
                                                break;
                                            }

                                            valid = ValidateOverflowPage(nextPage, eof, startOffset, ref mem);

                                            //we already advance the pointer inside the validation
                                            if (valid == false) break;
                                        }

                                        if (valid == false)
                                        {
                                            //The first page was valid so we can skip the entire overflow
                                            mem += numberOfPages * _pageSize;
                                            continue;
                                        }

                                        rc = Sodium.crypto_generichash_final(hashStatePtr, hashResultPtr, (UIntPtr)_streamHashResult.Length);
                                        if (rc != 0)
                                        {
                                            if (_logger.IsOperationsEnabled)
                                                _logger.Operations(
                                                    $"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to compute attachment hash, will skip it.");
                                            mem += numberOfPages * _pageSize;
                                            continue;
                                        }

                                        var hash = new string(' ', 44);
                                        fixed (char* p = hash)
                                        {
                                            var len = Base64.ConvertToBase64Array(p, hashResultPtr, 0, 32);
                                            Debug.Assert(len == 44);
                                        }

                                        var tmpFile = Path.GetTempFileName();
                                        if (File.Exists(tmpFile))
                                            File.Delete(tmpFile);
                                        using (var fs = new FileStream(tmpFile, FileMode.Create, FileAccess.ReadWrite))
                                        {
                                            foreach (var chunk in _attachmentChunks)
                                            {
                                                var buffer = new Span<byte>(chunk.Ptr.ToPointer(), chunk.Size);
                                                fs.Write(buffer);
                                                fs.Flush();
                                            }

                                            fs.Position = 0;
                                            _numberOfAttachmentsRetrieved++;
                                            _lastRecoveredKey = $"Attachment with hash '{hash}'";
                                            recoveredTool.WriteAttachment(hash, "Recovered_" + Guid.NewGuid(), "", fs, totalSize);
                                            fs.Close();
                                        }
                                        File.Delete(tmpFile);
                                    }

                                    mem += numberOfPages * _pageSize;
                                }

                                else if (Write(recoveredTool, (byte*)pageHeader + PageHeader.SizeOf, pageHeader->OverflowSize,
                                    context, startOffset, ((RawDataOverflowPageHeader*)page)->TableType))
                                {
                                    mem += numberOfPages * _pageSize;
                                }
                                else //write document failed
                                {
                                    mem += _pageSize;
                                }

                                continue;
                            }

                            //We don't have checksum for encrypted pages
                            if (IsEncrypted == false)
                            {
                                ulong checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, 0);

                                if (checksum != pageHeader->Checksum)
                                {
                                    CheckInvalidPagesInARaw(pageHeader, mem);
                                    var message =
                                        $"Invalid checksum for page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    continue;
                                }

                                _shouldIgnoreInvalidPagesInARaw = true;
                            }

                            // small raw data section
                            var rawHeader = (RawDataSmallPageHeader*)page;

                            // small raw data section header
                            if (rawHeader->RawDataFlags.HasFlag(RawDataPageFlags.Header))
                            {
                                mem += _pageSize;
                                continue;
                            }

                            if (rawHeader->NextAllocation > _pageSize)
                            {
                                var message =
                                    $"RawDataSmallPage #{rawHeader->PageNumber} at {GetFilePosition(startOffset, mem)} next allocation is larger than {_pageSize} bytes";
                                mem = PrintErrorAndAdvanceMem(message, mem);
                                continue;
                            }

                            for (var pos = PageHeader.SizeOf; pos < rawHeader->NextAllocation;)
                            {
                                var currMem = page + pos;
                                var entry = (RawDataSection.RawDataEntrySizes*)currMem;
                                //this indicates that the current entry is invalid because it is outside the size of a page
                                if (pos > _pageSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, mem + pos)}";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }

                                //Allocated size of entry exceed the bound of the page next allocation
                                if (entry->AllocatedSize + pos + sizeof(RawDataSection.RawDataEntrySizes) >
                                    rawHeader->NextAllocation)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, mem + pos)}" +
                                        "the allocated entry exceed the bound of the page next allocation.";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }

                                if (entry->UsedSize > entry->AllocatedSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, mem + pos)}" +
                                        "the size of the entry exceed the allocated size";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }

                                pos += entry->AllocatedSize + sizeof(RawDataSection.RawDataEntrySizes);
                                if (entry->AllocatedSize == 0 || entry->UsedSize == -1)
                                    continue;

                                if (Write(recoveredTool, currMem + sizeof(RawDataSection.RawDataEntrySizes), entry->UsedSize,
                                        context, startOffset, ((RawDataSmallPageHeader*)page)->TableType) == false)
                                    break;
                            }

                            mem += _pageSize;
                        }
                        catch (InvalidOperationException ioe) when (ioe.Message == EncryptedDatabaseWithoutMasterKeyErrorMessage)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            var message =
                                $"Unexpected exception at position {GetFilePosition(startOffset, mem)}:{Environment.NewLine} {e}";
                            mem = PrintErrorAndAdvanceMem(message, mem);
                            try
                            {
                                recoveredTool.Log(message, e);
                            }
                            catch (Exception)
                            {
                                // ignore
                            }
                        }

                    PrintRecoveryProgress(startOffset, mem, eof, DateTime.UtcNow);

                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(Environment.NewLine +
                                           $"Discovered a total of {_numberOfDocumentsRetrieved:#,#;00} documents within {sw.Elapsed.TotalSeconds::#,#.#;;00} seconds." +
                                           Environment.NewLine +
                                           $"Discovered a total of {_numberOfAttachmentsRetrieved:#,#;00} attachments. " + Environment.NewLine +
                                           $"Discovered a total of {_numberOfRevisionsRetrieved:#,#;00} revisions. " + Environment.NewLine +
                                           $"Discovered a total of {_numberOfConflictsRetrieved:#,#;00} conflicts. " + Environment.NewLine +
                                           $"Discovered a total of {_numberOfCountersRetrieved:#,#;00} counters. " + Environment.NewLine +
                                           $"Discovered a total of {_numberOfFaultedPages::#,#;00} faulted pages.");
                }

                if (_cancellationRequested)
                    return RecoveryStatus.CancellationRequested;
                return RecoveryStatus.Success;
            }
            finally
            {
                tx?.Dispose();
                se?.Dispose();
                if (_config.LoggingMode != LogMode.None)
                    LoggingSource.Instance.EndLogging();
            }
        }

        private void CheckInvalidPagesInARaw(PageHeader* pageHeader, byte* mem)
        {
            if (_shouldIgnoreInvalidPagesInARaw)
                return;

            if (MacNotZero(pageHeader))
                if (MaxNumberOfInvalidChecksumWithNoneZeroMac <= _invalidChecksumWithNoneZeroMac++)
                {
                    PrintErrorAndAdvanceMem(EncryptedDatabaseWithoutMasterKeyErrorMessage, mem);
                    throw new InvalidOperationException(EncryptedDatabaseWithoutMasterKeyErrorMessage);
                }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MacNotZero(PageHeader* pageHeader)
        {
            byte* zeroes = stackalloc byte[SizeOfMacInBytes];
            return Memory.Compare(zeroes, pageHeader->Mac, SizeOfMacInBytes) != 0;
        }


        private byte* DecryptPageIfNeeded(byte* mem, long start, ref TempPagerTransaction tx, bool maybePulseTransaction = false)
        {
            if (IsEncrypted == false)
                return mem;

            //We must make sure we can close the transaction since it may hold buffers for memory we still need e.g. attachments chunks.
            if (maybePulseTransaction && tx?.TotalEncryptionBufferSize > _maxTransactionSize)
            {
                tx.Dispose();
                tx = new TempPagerTransaction();
            }

            long pageNumber = ((PageHeader*)mem)->PageNumber;
            var res = Pager.AcquirePagePointer(tx, pageNumber);

            return res;
        }

        private static void ExtractTagFromLastPage(PageHeader* nextPage, StreamPageHeader* streamPageHeader, ref string tag)
        {
            var si = (StreamInfo*)((byte*)nextPage + streamPageHeader->ChunkSize + PageHeader.SizeOf);
            var tagSize = si->TagSize;
            if (nextPage->OverflowSize > tagSize + streamPageHeader->ChunkSize + StreamInfo.SizeOf)
                //not sure if we should fail because of missing tag
                return;
            if (tagSize > 0) tag = Encodings.Utf8.GetString((byte*)si + StreamInfo.SizeOf, tagSize);
        }

        private bool ValidateOverflowPage(PageHeader* pageHeader, byte* eof, long startOffset, ref byte* mem)
        {
            //pageHeader might be a buffer address we need to verify we don't exceed the original memory boundary here
            var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            var sizeOfPages = numberOfPages * _pageSize;
            var endOfOverflow = (long)mem + sizeOfPages;
            // the endOfOverflow can be equal to eof if the last page is overflow
            if (endOfOverflow > (long)eof)
            {
                var message =
                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                    $" size exceeds the end of the file ([{(long)mem}:{endOfOverflow}])";
                mem = PrintErrorAndAdvanceMem(message, mem);
                return false;
            }

            if (pageHeader->OverflowSize <= 0)
            {
                var message =
                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                    $" OverflowSize is not a positive number ({pageHeader->OverflowSize})";
                mem = PrintErrorAndAdvanceMem(message, mem);
                return false;
            }

            if (IsEncrypted == false)
            {
                // this can only be here if we know that the overflow size is valid
                ulong checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, pageHeader->OverflowSize);

                if (checksum != pageHeader->Checksum)
                {
                    CheckInvalidPagesInARaw(pageHeader, mem);
                    var message =
                        $"Invalid checksum for overflow page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                    mem = PrintErrorAndAdvanceMem(message, mem);
                    return false;
                }

                _shouldIgnoreInvalidPagesInARaw = true;
            }

            return true;
        }

        private bool Write(RecoveredDatabaseCreator recoveryTool, byte* mem, int sizeInBytes, JsonOperationContext context, long startOffset, byte tableType)
        {
            switch ((TableType)tableType)
            {
                case TableType.None:
                    return false;
                case TableType.Documents:
                    _numberOfDocumentsRetrieved++;
                    return WriteDocument(recoveryTool, mem, sizeInBytes, context, startOffset);
                case TableType.Revisions:
                    _numberOfRevisionsRetrieved++;
                    return WriteRevision(recoveryTool, mem, sizeInBytes, context, startOffset);
                case TableType.Conflicts:
                    _numberOfConflictsRetrieved++;
                    return WriteConflict(recoveryTool, mem, sizeInBytes, context, startOffset);
                case TableType.Counters:
                    _numberOfCountersRetrieved++;
                    return WriteCounter(recoveryTool, mem, sizeInBytes, context, startOffset);
                default:
                    throw new ArgumentOutOfRangeException(nameof(tableType), tableType, null);
            }
        }

        private bool WriteCounter(RecoveredDatabaseCreator recoveryTool, byte* mem, int sizeInBytes, JsonOperationContext context,
            long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                CounterGroupDetail counterGroup = null;
                try
                {
                    counterGroup = CountersStorage.TableValueToCounterGroupDetail(context, ref tvr);
                    if (counterGroup == null)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to convert table value to counter at position {GetFilePosition(startOffset, mem)}");
                        return false;
                    }
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(
                            $"Found invalid counter item at position={GetFilePosition(startOffset, mem)} with document Id={counterGroup?.DocumentId ?? "null"} and counter values={counterGroup?.Values}{Environment.NewLine}{e}");
                    return false;
                }

                _lastRecoveredKey = $"Counter of '{counterGroup.DocumentId}'";
                recoveryTool.WriteCounterItem(counterGroup);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found counter item with document Id={counterGroup.DocumentId} and counter values={counterGroup.Values}");

                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing counter item at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private bool WriteDocument(RecoveredDatabaseCreator recoveryTool, byte* mem, int sizeInBytes, JsonOperationContext context,
            long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                Document document = null;
                try
                {
                    document = DocumentsStorage.ParseRawDataSectionDocumentWithValidation(context, ref tvr, sizeInBytes);
                    if (document == null)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to convert table value to document at position {GetFilePosition(startOffset, mem)}");
                        return false;
                    }

                    // document.EnsureMetadata();
                    document.Data.BlittableValidation();

                    if (_previouslyWrittenDocs.TryGetValue(document.Id, out var previousEtag))
                        // This is a duplicate doc. It can happen when a page is marked as freed, but still exists in the data file.
                        // We determine which one to choose by their etag. If the document is newer, we will write it again to the
                        // smuggler file. This way, when importing, it will be the one chosen (last write wins)
                        if (document.Etag <= previousEtag)
                            return false;

                    _previouslyWrittenDocs[document.Id] = document.Etag;
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(
                            $"Found invalid blittable document at pos={GetFilePosition(startOffset, mem)} with key={document?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found document with key={document.Id}");

                _lastRecoveredKey = $"Document '{document.Id}'";
                recoveryTool.WriteDocument(document);
                document.Dispose();

                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing document at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private bool WriteRevision(RecoveredDatabaseCreator recoveryTool, byte* mem, int sizeInBytes, JsonOperationContext context,
            long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                Document revision = null;
                try
                {
                    revision = RevisionsStorage.ParseRawDataSectionRevisionWithValidation(context, ref tvr, sizeInBytes, out var changeVector);
                    if (revision == null)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to convert table value to revision document at position {GetFilePosition(startOffset, mem)}");
                        return false;
                    }

                    // revision.EnsureMetadata();
                    revision.Data.BlittableValidation();
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(
                            $"Found invalid blittable revision document at pos={GetFilePosition(startOffset, mem)} with key={revision?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                _lastRecoveredKey = $"Revision '{revision.Id}'";
                recoveryTool.WriteRevision(revision);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found revision document with key={revision.Id}");
                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing revision document at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private bool WriteConflict(RecoveredDatabaseCreator recoveryTool, byte* mem, int sizeInBytes, JsonOperationContext context,
            long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                DocumentConflict conflict = null;
                try
                {
                    conflict = ConflictsStorage.ParseRawDataSectionConflictWithValidation(context, ref tvr, sizeInBytes, out var changeVector);
                    if (conflict == null)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to convert table value to conflict document at position {GetFilePosition(startOffset, mem)}");
                        return false;
                    }

                    conflict.Doc.BlittableValidation();
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(
                            $"Found invalid blittable conflict document at pos={GetFilePosition(startOffset, mem)} with key={conflict?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                _lastRecoveredKey = $"Conflict '{conflict.Id}'";
                recoveryTool.WriteConflict(conflict);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found conflict document with key={conflict.Id}");
                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing conflict document at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* PrintErrorAndAdvanceMem(string message, byte* mem)
        {
            if (_logger.IsOperationsEnabled) _logger.Operations(message);
            _numberOfFaultedPages++;
            return mem + _pageSize;
        }
    }
}

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl.Paging;
using static System.String;
using static Voron.Data.BTrees.Tree;
using Constants = Voron.Global.Constants;

namespace Voron.Recovery
{
    public unsafe class Recovery : IDisposable
    {
        public Recovery(VoronRecoveryConfiguration config)
        {
            _datafile = config.PathToDataFile;
            _output = config.OutputFileName;
            _pageSize = config.PageSizeInKB * Constants.Size.Kilobyte;
            _initialContextSize = config.InitialContextSizeInMB * Constants.Size.Megabyte;
            _initialContextLongLivedSize = config.InitialContextLongLivedSizeInKB * Constants.Size.Kilobyte;

            _masterKey = config.MasterKey;

            // by default CopyOnWriteMode will be true
            _copyOnWrite = !config.DisableCopyOnWriteMode;
            _config = config;
            _option = CreateOptions();

            _progressIntervalInSec = config.ProgressIntervalInSec;
            _previouslyWrittenDocs = new Dictionary<string, long>(OrdinalIgnoreCaseStringStructComparer.Instance);
            if (config.LoggingMode != LogMode.None)
                LoggingSource.Instance.SetupLogMode(config.LoggingMode, Path.Combine(Path.GetDirectoryName(_output), LogFileName), TimeSpan.FromDays(3), long.MaxValue, false);
            _logger = LoggingSource.Instance.GetLogger<Recovery>("Voron Recovery");
            _shouldIgnoreInvalidPagesInARaw = config.IgnoreInvalidPagesInARow;
        }

        private StorageEnvironmentOptions CreateOptions()
        {
            var result = StorageEnvironmentOptions.ForPath(_config.DataFileDirectory, null, null, null, null);
            result.CopyOnWriteMode = _copyOnWrite;
            result.ManualFlushing = true;
            result.ManualSyncing = true;
            result.IgnoreInvalidJournalErrors = _config.IgnoreInvalidJournalErrors;
            result.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = _config.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;
            result.Encryption.MasterKey = _masterKey;
            return result;
        }

        private readonly byte[] _streamHashState = new byte[(int)Sodium.crypto_generichash_statebytes()];
        private readonly byte[] _streamHashResult = new byte[(int)Sodium.crypto_generichash_bytes()];

        private const int SizeOfMacInBytes = 16;
        private readonly List<(IntPtr Ptr, int Size)> _attachmentChunks = new List<(IntPtr Ptr, int Size)>();
        private readonly VoronRecoveryConfiguration _config;

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
                    $"{now:hh:MM:ss}: Recovering page at position {currPos:#,#;;0}/{eofPos:#,#;;0} ({(double)currPos / eofPos:p}) - Last recovered doc is {_lastRecoveredDocumentKey}");
            }

            StorageEnvironment se = null;
            TempPagerTransaction tx = null;
            try
            {
                if (IsEncrypted)
                {
                    //We need a tx for the encryption pager and we can't dispose of it while reading the page.
                    tx = new TempPagerTransaction();
                }
                var sw = new Stopwatch();
                sw.Start();
                byte* mem = null;
                if (_copyOnWrite)
                {
                    writer.WriteLine($"Recovering journal files from folder '{_option.JournalPath}', this may take a while...");

                    bool optionOwnsPagers = _option.OwnsPagers;
                    try
                    {
                        _option.OwnsPagers = false;

                        while (true)
                        {
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
                            writer.WriteLine("Please note that this is usafe operation and we highly recommend to backup you files.");

                            throw;
                        }
                        writer.WriteLine("Journal recovery failed, don't worry we will continue with data recovery.");
                        writer.WriteLine("The reason for the Jornal recovery failure was:");
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
                    {
                        mem = _option.DataPager.PagerState.MapBase;
                    }
                    else
                    {
                        mem = se.Options.DataPager.PagerState.MapBase;
                    }
                }
                long startOffset = (long)mem;
                var fi = new FileInfo(_datafile);
                var fileSize = fi.Length;
                //making sure eof is page aligned
                var eof = mem + (fileSize / _pageSize) * _pageSize;

                DateTime lastProgressReport = DateTime.MinValue;

                if (Directory.Exists(Path.GetDirectoryName(_output)) == false)
                    Directory.CreateDirectory(Path.GetDirectoryName(_output));

                using (var destinationStreamDocuments = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-2-Documents" + Path.GetExtension(_output))))
                using (var destinationStreamRevisions = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-3-Revisions" + Path.GetExtension(_output))))
                using (var destinationStreamConflicts = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-4-Conflicts" + Path.GetExtension(_output))))
                using (var destinationStreamCounters = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-5-Counters" + Path.GetExtension(_output))))
                using (var destinationStreamTimeSeries = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-6-TimeSeries" + Path.GetExtension(_output))))
                using (var gZipStreamDocuments = new GZipStream(destinationStreamDocuments, CompressionMode.Compress, true))
                using (var gZipStreamRevisions = new GZipStream(destinationStreamRevisions, CompressionMode.Compress, true))
                using (var gZipStreamConflicts = new GZipStream(destinationStreamConflicts, CompressionMode.Compress, true))
                using (var gZipStreamCounters = new GZipStream(destinationStreamCounters, CompressionMode.Compress, true))
                using (var gZipStreamTimeSeries = new GZipStream(destinationStreamTimeSeries, CompressionMode.Compress, true))
                using (var context = new JsonOperationContext(_initialContextSize, _initialContextLongLivedSize, 8 * 1024, SharedMultipleUseFlag.None))
                using (var documentsWriter = new BlittableJsonTextWriter(context, gZipStreamDocuments))
                using (var revisionsWriter = new BlittableJsonTextWriter(context, gZipStreamRevisions))
                using (var conflictsWriter = new BlittableJsonTextWriter(context, gZipStreamConflicts))
                using (var countersWriter = new BlittableJsonTextWriter(context, gZipStreamCounters))
                using (var timeSeriesWriter = new BlittableJsonTextWriter(context, gZipStreamTimeSeries))
                {
                    WriteSmugglerHeader(documentsWriter, ServerVersion.Build, "Docs");
                    WriteSmugglerHeader(revisionsWriter, ServerVersion.Build, nameof(DatabaseItemType.RevisionDocuments));
                    WriteSmugglerHeader(conflictsWriter, ServerVersion.Build, nameof(DatabaseItemType.Conflicts));
                    WriteSmugglerHeader(countersWriter, ServerVersion.Build, nameof(DatabaseItemType.CounterGroups));
                    WriteSmugglerHeader(timeSeriesWriter, ServerVersion.Build, nameof(DatabaseItemType.TimeSeries));

                    while (mem < eof)
                    {
                        try
                        {
                            var page = DecryptPageIfNeeded(mem, startOffset, ref tx, maybePulseTransaction: true);

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
                                if (lastProgressReport != DateTime.MinValue)
                                {
                                    writer.WriteLine("Press 'q' to quit the recovery process");
                                }

                                lastProgressReport = now;
                                PrintRecoveryProgress(startOffset, mem, eof, now);
                            }

                            var pageHeader = (PageHeader*)page;

                            //this page is not raw data section move on
                            if ((pageHeader->Flags).HasFlag(PageFlags.RawData) == false && pageHeader->Flags.HasFlag(PageFlags.Stream) == false)
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
                            ulong checksum;
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

                                    int rc;
                                    fixed (byte* hashStatePtr = _streamHashState)
                                    fixed (byte* hashResultPtr = _streamHashResult)
                                    {
                                        long totalSize = 0;
                                        _attachmentChunks.Clear();
                                        rc = Sodium.crypto_generichash_init(hashStatePtr, null, UIntPtr.Zero, (UIntPtr)_streamHashResult.Length);
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
                                            nextPage = (PageHeader*)DecryptPageIfNeeded(nextStreamHeader, startOffset, ref tx, false);

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
                                            if (valid == false)
                                            {
                                                break;
                                            }
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

                                        WriteAttachment(documentsWriter, totalSize, hash, tag);
                                    }

                                    mem += numberOfPages * _pageSize;
                                }
                                else
                                {
                                    var ptr = (byte*)pageHeader + PageHeader.SizeOf;
                                    int sizeInBytes = pageHeader->OverflowSize;
                                    byte* buffer = null;
                                    try
                                    {
                                        if (pageHeader->Flags.HasFlag(PageFlags.Compressed))
                                        {
                                            buffer = Decompress(ref ptr, ref sizeInBytes);
                                        }
                                        if (Write((byte*)pageHeader + PageHeader.SizeOf, pageHeader->OverflowSize,
                                            documentsWriter, revisionsWriter,
                                            conflictsWriter, countersWriter, timeSeriesWriter,
                                            context, startOffset,
                                            ((RawDataOverflowPageHeader*)page)->TableType))
                                        {
                                            mem += numberOfPages * _pageSize;
                                        }
                                        else //write document failed
                                        {
                                            mem += _pageSize;
                                        }
                                    }
                                    finally
                                    {
                                        if (buffer != null)
                                            Marshal.FreeHGlobal((IntPtr)buffer);
                                    }
                                }

                                continue;
                            }

                            //We don't have checksum for encrypted pages
                            if (IsEncrypted == false)
                            {
                                checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, 0);

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

                                pos += entry->AllocatedSize + sizeof(RawDataSection.RawDataEntrySizes);
                                if (entry->AllocatedSize == 0 || entry->IsFreed)
                                    continue;

                                if (entry->UsedSize > entry->AllocatedSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, mem + pos)}" +
                                        "the size of the entry exceed the allocated size";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }

                                int sizeInBytes = entry->UsedSize;
                                byte* ptr = currMem + sizeof(RawDataSection.RawDataEntrySizes);
                                byte* buffer = null;
                                if (entry->IsCompressed)
                                {
                                    buffer = Decompress(ref ptr, ref sizeInBytes);
                                }

                                try
                                {
                                    if (Write(ptr,
                                        sizeInBytes,
                                        documentsWriter, revisionsWriter,
                                        conflictsWriter, countersWriter, timeSeriesWriter,
                                        context, startOffset, ((RawDataSmallPageHeader*)page)->TableType) == false)
                                        break;
                                }
                                finally
                                {
                                    if (buffer != null)
                                        Marshal.FreeHGlobal((IntPtr)buffer);
                                }
                            }

                            mem += _pageSize;
                        }
                        catch (InvalidOperationException ioe) when (ioe.Message == EncryptedDatabaseWithoutMasterkeyErrorMessage)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            var message =
                                $"Unexpected exception at position {GetFilePosition(startOffset, mem)}:{Environment.NewLine} {e}";
                            mem = PrintErrorAndAdvanceMem(message, mem);
                        }
                    }

                    PrintRecoveryProgress(startOffset, mem, eof, DateTime.UtcNow);

                    ReportOrphanAttachmentsAndMissingAttachments(writer, documentsWriter, ct);
                    //This will only be the case when we don't have orphan attachments and we wrote the last attachment after we wrote the
                    //last document
                    if (_lastWriteIsDocument == false && _lastAttachmentInfo.HasValue)
                    {
                        WriteDummyDocumentForAttachment(documentsWriter, _lastAttachmentInfo.Value.hash, _lastAttachmentInfo.Value.size, _lastAttachmentInfo.Value.tag);
                    }

                    ReportOrphanCountersAndMissingCounters(writer, documentsWriter, ct);
                    ReportOrphanTimeSeriesAndMissingTimeSeries(writer, documentsWriter, ct);

                    documentsWriter.WriteEndArray();
                    conflictsWriter.WriteEndArray();
                    revisionsWriter.WriteEndArray();
                    countersWriter.WriteEndArray();
                    timeSeriesWriter.WriteEndArray();
                    documentsWriter.WriteEndObject();
                    conflictsWriter.WriteEndObject();
                    revisionsWriter.WriteEndObject();
                    countersWriter.WriteEndObject();
                    timeSeriesWriter.WriteEndObject();

                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations(Environment.NewLine +
                            $"Discovered a total of {_numberOfDocumentsRetrieved:#,#;00} documents within {sw.Elapsed.TotalSeconds::#,#.#;;00} seconds." + Environment.NewLine +
                            $"Discovered a total of {_attachmentsHashs.Count:#,#;00} attachments. " + Environment.NewLine +
                            $"Discovered a total of {_numberOfCountersRetrieved:#,#;00} counters. " + Environment.NewLine +
                            $"Discovered a total of {_numberOfTimeSeriesSegmentsRetrieved:#,#;00} time-series segments. " + Environment.NewLine +
                            $"Discovered a total of {_numberOfFaultedPages::#,#;00} faulted pages.");
                    }
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

        private Dictionary<int, ZstdLib.CompressionDictionary> _dictionaries = null;

        private byte* Decompress(ref byte* ptr, ref int sizeInBytes)
        {
            var dicId = BlittableJsonReaderBase.ReadVariableSizeIntInReverse(ptr,
                sizeInBytes - 1,
                out var offset);

            var data = new ReadOnlySpan<byte>(ptr, sizeInBytes - offset);
            var outSize = ZstdLib.GetDecompressedSize(data);
            var buffer = (byte*)Marshal.AllocHGlobal(outSize);
            if (buffer == null)
                throw new OutOfMemoryException();

            ZstdLib.CompressionDictionary dictionary;
            if (dicId == 0)
            {
                dictionary = null;
            }
            else
            {
                if (_dictionaries == null)
                {
                    LoadCompressionDictionaries();
                }
                if (_dictionaries.TryGetValue(dicId, out dictionary) == false)
                {
                    throw new InvalidDataException("Unable to find dictionary " + dicId + " from the recovery files, unable to handle compressed data");
                }
            }

            var output = ZstdLib.Decompress(data, new Span<byte>(buffer, outSize), dictionary);
            if (output != outSize)
                throw new InvalidDataException("Bad size after decompression");
            sizeInBytes = output;
            ptr = buffer;
            return buffer;
        }

        private void LoadCompressionDictionaries()
        {
            _dictionaries = new Dictionary<int, ZstdLib.CompressionDictionary>();
            try
            {
                var subKeyLen = (int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
                var subKey = stackalloc byte[subKeyLen];

                var recoveryFiles = Directory.GetFiles(_config.DataFileDirectory, TableValueCompressor.CompressionRecoveryExtensionGlob);
                foreach (string recoveryFile in recoveryFiles)
                {
                    try
                    {
                        using var fs = File.OpenRead(recoveryFile);
                        using var zip = new ZipArchive(fs);

                        foreach (ZipArchiveEntry entry in zip.Entries)
                        {
                            if (Path.GetExtension(entry.Name) != ".dic")
                                continue;

                            if (int.TryParse(Path.GetFileNameWithoutExtension(entry.Name), out var id) == false)
                                continue;

                            if (_dictionaries.ContainsKey(id))
                                continue; // read from the previous file
                            var ms = new MemoryStream();
                            using var s = entry.Open();
                            s.CopyTo(ms);

                            fixed (byte* pKey = _config.MasterKey)
                            fixed (byte* b = ms.ToArray())
                            fixed (byte* ctx = TableValueCompressor.EncryptionContext)
                            {
                                var nonceEntry = zip.GetEntry(Path.GetFileNameWithoutExtension(entry.Name) + ".nonce");
                                if (nonceEntry != null)
                                {
                                    if (_config.MasterKey == null)
                                    {
                                        throw new InvalidOperationException("The compression dictionaries are encrypted, but you didn't specify a master key for the recovery.");
                                    }

                                    var macEntry = zip.GetEntry(Path.GetFileNameWithoutExtension(entry.Name) + ".mac");
                                    byte[] nonceBuffer = new BinaryReader(nonceEntry.Open()).ReadBytes((int)Sodium.crypto_stream_xchacha20_noncebytes());
                                    byte[] macBuffer = new BinaryReader(macEntry.Open()).ReadBytes((int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes());

                                    if (nonceBuffer.Length != (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes())
                                        throw new InvalidOperationException("Invalid nonce length");

                                    if (macBuffer.Length != (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes())
                                        throw new InvalidOperationException("Invalid mac length");

                                    if (Sodium.crypto_kdf_derive_from_key(subKey, (UIntPtr)subKeyLen, (ulong)id, ctx, pKey) != 0)
                                        throw new InvalidOperationException("Unable to generate derived key");

                                    fixed (byte* nonce = nonceBuffer)
                                    fixed (byte* mac = macBuffer)
                                    {
                                        var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
                                            b,
                                            null,
                                            b,
                                            (ulong)ms.Length,
                                            mac,
                                            null,
                                            0,
                                            nonce,
                                            subKey
                                        );

                                        if (rc != 0)
                                            throw new InvalidDataException("Unable to decrypt dictionary " + id);
                                    }
                                }

                                _dictionaries[id] = new ZstdLib.CompressionDictionary(id,
                                    b + sizeof(CompressionDictionaryInfo),
                                    (int)ms.Length - sizeof(CompressionDictionaryInfo), 3);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations("Failed to read " + recoveryFile + " dictionary, ignoring and will continue. Some compressed data may not be recoverable", e);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations("Failed to read recovery dictionaries from " + _config.DataFileDirectory + ". Compressed data will not be recovered!", e);
                }
            }
        }

        private void CheckInvalidPagesInARaw(PageHeader* pageHeader, byte* mem)
        {
            if (_shouldIgnoreInvalidPagesInARaw)
                return;

            if (MacNotZero(pageHeader))
            {
                if (MaxNumberOfInvalidChecksumWithNoneZeroMac <= _InvalidChecksumWithNoneZeroMac++)
                {
                    PrintErrorAndAdvanceMem(EncryptedDatabaseWithoutMasterkeyErrorMessage, mem);
                    throw new InvalidOperationException(EncryptedDatabaseWithoutMasterkeyErrorMessage);
                }
            }
        }

        private const string EncryptedDatabaseWithoutMasterkeyErrorMessage =
            "this is a strong indication that you're recovering an encrypted database and didn't" +
            " provide the encryption key using the  '--MasterKey=<KEY>' command line flag";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MacNotZero(PageHeader* pageHeader)
        {
            byte* zeroes = stackalloc byte[SizeOfMacInBytes];
            return Sparrow.Memory.Compare(zeroes, pageHeader->Mac, SizeOfMacInBytes) != 0;
        }

        private Size _maxTransactionSize = new Size(64, SizeUnit.Megabytes);

        private byte* DecryptPageIfNeeded(byte* mem, long start, ref TempPagerTransaction tx, bool maybePulseTransaction = false)
        {
            if (IsEncrypted == false)
                return mem;

            //We must make sure we can close the transaction since it may hold buffers for memory we still need e.g. attachments chunks.
            if (maybePulseTransaction && tx?.AdditionalMemoryUsageSize > _maxTransactionSize)
            {
                tx.Dispose();
                tx = new TempPagerTransaction();
            }

            long pageNumber = (long)((PageHeader*)mem)->PageNumber;
            var res = Pager.AcquirePagePointer(tx, pageNumber);

            return res;
        }

        private static void ExtractTagFromLastPage(PageHeader* nextPage, StreamPageHeader* streamPageHeader, ref string tag)
        {
            var si = (StreamInfo*)((byte*)nextPage + streamPageHeader->ChunkSize + PageHeader.SizeOf);
            var tagSize = si->TagSize;
            if (nextPage->OverflowSize > tagSize + streamPageHeader->ChunkSize + StreamInfo.SizeOf)
            {
                //not sure if we should fail because of missing tag
                return;
            }
            if (tagSize > 0)
            {
                tag = Encodings.Utf8.GetString((byte*)si + StreamInfo.SizeOf, tagSize);
            }
        }

        private const string EmptyCollection = "@empty";
        private static readonly char[] TagSeparator = { (char)SpecialChars.RecordSeparator };

        private void WriteDummyDocumentForAttachment(BlittableJsonTextWriter writer, string hash, long size, string tag)
        {
            if (_documentWritten)
                writer.WriteComma();
            //start metadata
            writer.WriteStartObject();
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Key);
            writer.WriteStartObject();
            //collection name
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Collection);
            writer.WriteString(EmptyCollection);
            writer.WriteComma();
            //id
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Id);
            writer.WriteString($"DummyDoc{_dummyDocNumber++}");
            writer.WriteComma();
            //change vector
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.ChangeVector);
            writer.WriteString(Empty);
            writer.WriteComma();
            //flags
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Flags);
            writer.WriteString(DocumentFlags.HasAttachments.ToString());
            writer.WriteComma();
            //start attachment
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Attachments);
            //start attachment array
            writer.WriteStartArray();
            //start attachment object
            writer.WriteStartObject();
            if (tag != null)
            {
                //doc id | type 'd' or 'r' | name | hash | content type
                var tokens = tag.Split(TagSeparator);
                if (tokens.Length == 5)
                {
                    WriteAttachmentMetadata(writer, hash, size, tokens[2], tokens[4]);
                }
                else
                {
                    WriteAttachmentMetadata(writer, hash, size, $"DummyAttachmentName{_dummyAttachmentNumber++}", Empty);
                }
            }
            else
            {
                WriteAttachmentMetadata(writer, hash, size, $"DummyAttachmentName{_dummyAttachmentNumber++}", Empty);
            }
            //end attachment object
            writer.WriteEndObject();
            // end attachment array
            writer.WriteEndArray();
            //end attachment
            writer.WriteEndObject();
            //end metadata
            writer.WriteEndObject();
            _lastWriteIsDocument = true;
        }

        private static void WriteAttachmentMetadata(BlittableJsonTextWriter writer, string hash, long size, string name, string contentType)
        {
            //name
            writer.WritePropertyName("Name");
            writer.WriteString(name);
            writer.WriteComma();
            //hash
            writer.WritePropertyName("Hash");
            writer.WriteString(hash);
            writer.WriteComma();
            //content type
            writer.WritePropertyName("ContentType");
            writer.WriteString(contentType);
            writer.WriteComma();
            //size
            writer.WritePropertyName("size");
            writer.WriteInteger(size);
        }

        private void ReportOrphanAttachmentsAndMissingAttachments(TextWriter writer, BlittableJsonTextWriter documentsWriter, CancellationToken ct)
        {
            //No need to scare the user if there are no attachments in the dump
            if (_attachmentsHashs.Count == 0 && _documentsAttachments.Count == 0)
                return;
            if (_attachmentsHashs.Count == 0)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("No attachments were recovered but there are documents pointing to attachments.");
                return;
            }
            if (_documentsAttachments.Count == 0)
            {
                foreach (var (hash, tag, size) in _attachmentsHashs)
                {
                    ReportOrphanAttachmentDocumentId(hash, size, tag, documentsWriter);
                }
                return;
            }
            writer.WriteLine("Starting to compute orphan and missing attachments this may take a while.");
            if (ct.IsCancellationRequested)
            {
                return;
            }
            _attachmentsHashs.Sort((x, y) => Compare(x.hash, y.hash, StringComparison.Ordinal));
            if (ct.IsCancellationRequested)
            {
                return;
            }
            _documentsAttachments.Sort((x, y) => Compare(x.Hash, y.Hash, StringComparison.Ordinal));
            //We rely on the fact that the attachment hash are unique in the _attachmentsHashs list (no duplicated values).
            int index = 0;
            foreach (var (hash, docId, size) in _attachmentsHashs)
            {
                if (ct.IsCancellationRequested)
                {
                    return;
                }
                var foundEqual = false;
                while (_documentsAttachments.Count > index)
                {
                    var documentHash = _documentsAttachments[index].Hash;
                    var compareResult = Compare(hash, documentHash, StringComparison.Ordinal);
                    if (compareResult == 0)
                    {
                        index++;
                        foundEqual = true;
                        continue;
                    }
                    //this is the case where we have a document with a hash but no attachment with that hash
                    if (compareResult > 0)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Document {_documentsAttachments[index].DocId} contains attachment with hash {documentHash} but we were not able to recover such attachment.");
                        index++;
                        continue;
                    }
                    break;
                }
                if (foundEqual == false)
                {
                    ReportOrphanAttachmentDocumentId(hash, size, docId, documentsWriter);
                }
            }
        }

        private void ReportOrphanAttachmentDocumentId(string hash, long size, string tag, BlittableJsonTextWriter writer)
        {
            var msg = new StringBuilder($"Found orphan attachment with hash {hash}");
            if (tag != null)
            {
                msg.Append($" attachment tag = {tag}");
            }
            if (_logger.IsOperationsEnabled)
                _logger.Operations(msg.ToString());
            WriteDummyDocumentForAttachment(writer, hash, size, tag);
        }

        private void ReportOrphanTimeSeriesAndMissingTimeSeries(TextWriter writer, BlittableJsonTextWriter documentWriter, in CancellationToken ct)
        {
            //No need to scare the user if there are no time-series in the dump
            if (_uniqueTimeSeriesDiscovered.Count == 0 && _documentsTimeSeries.Count == 0)
                return;

            if (_uniqueTimeSeriesDiscovered.Count == 0)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("No time-series were recovered but there are documents pointing to time-series.");
                return;
            }

            if (_documentsTimeSeries.Count == 0)
            {
                foreach (var kvp in _uniqueTimeSeriesDiscovered)
                {
                    if (_previouslyWrittenDocs.ContainsKey(kvp.Key))
                        continue;

                    // orphan time-series
                    WriteDummyDocumentForTimeSeries(documentWriter, kvp.Key, kvp.Value);
                }
                return;
            }
            writer.WriteLine("Starting to compute orphan and missing time-series. this may take a while.");
            if (ct.IsCancellationRequested)
                return;

            _documentsTimeSeries.Sort((x, y) => Compare(x.DocId + SpecialChars.RecordSeparator + x.Name,
                y.DocId + SpecialChars.RecordSeparator + y.Name, StringComparison.OrdinalIgnoreCase));
            //We rely on the fact that the time-series id+name is unique in the _uniqueTimeSeriesDiscovered list (no duplicated values).
            int index = 0;
            foreach (var (docId, names) in _uniqueTimeSeriesDiscovered)
            {
                if (_previouslyWrittenDocs.ContainsKey(docId) == false)
                {
                    // orphan time-series
                    WriteDummyDocumentForTimeSeries(documentWriter, docId, names);
                }

                // check for missing time-series
                foreach (var name in names)
                {
                    var discoveredKey = docId + SpecialChars.RecordSeparator + name;
                    if (ct.IsCancellationRequested)
                        return;

                    while (_documentsTimeSeries.Count > index)
                    {
                        var timeSeriesKey = _documentsTimeSeries[index].DocId + SpecialChars.RecordSeparator + _documentsTimeSeries[index].Name;
                        var compareResult = Compare(discoveredKey, timeSeriesKey, StringComparison.OrdinalIgnoreCase);
                        if (compareResult < 0)
                            break;

                        if (compareResult > 0)
                        {
                            // missing time-series - found a document with time-series that wasn't recovered
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"Document {_documentsTimeSeries[index].DocId} contains a time-series with name {_documentsTimeSeries[index].Name} but we were not able to recover such time-series.");
                        }

                        index++;
                    }
                }
            }
        }

        private void ReportOrphanCountersAndMissingCounters(TextWriter writer, BlittableJsonTextWriter documentWriter, CancellationToken ct)
        {
            //No need to scare the user if there are no counters in the dump
            if (_uniqueCountersDiscovered.Count == 0 && _documentsCounters.Count == 0)
                return;

            if (_uniqueCountersDiscovered.Count == 0)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("No counters were recovered but there are documents pointing to counters.");
                return;
            }

            if (_documentsCounters.Count == 0)
            {
                foreach (var kvp in _uniqueCountersDiscovered)
                {
                    if (_previouslyWrittenDocs.ContainsKey(kvp.Key))
                        continue;

                    // orphan counters
                    WriteDummyDocumentForCounters(documentWriter, kvp.Key, kvp.Value);
                }
                return;
            }
            writer.WriteLine("Starting to compute orphan and missing counters. this may take a while.");
            if (ct.IsCancellationRequested)
                return;

            _documentsCounters.Sort((x, y) => Compare(x.DocId + SpecialChars.RecordSeparator + x.Name,
                y.DocId + SpecialChars.RecordSeparator + y.Name, StringComparison.OrdinalIgnoreCase));
            //We rely on the fact that the counter id+name is unique in the _discoveredCounters list (no duplicated values).
            int index = 0;
            foreach (var (docId, names) in _uniqueCountersDiscovered)
            {
                if (_previouslyWrittenDocs.ContainsKey(docId) == false)
                {
                    // orphan counters
                    WriteDummyDocumentForCounters(documentWriter, docId, names);
                }

                // check for missing counters
                foreach (var name in names)
                {
                    var discoveredKey = docId + SpecialChars.RecordSeparator + name;
                    if (ct.IsCancellationRequested)
                        return;

                    while (_documentsCounters.Count > index)
                    {
                        var documentsCountersKey = _documentsCounters[index].DocId + SpecialChars.RecordSeparator + _documentsCounters[index].Name;
                        var compareResult = Compare(discoveredKey, documentsCountersKey, StringComparison.OrdinalIgnoreCase);
                        if (compareResult < 0)
                            break;

                        if (compareResult > 0)
                        {
                            // missing counter - found a document with a counter that wasn't recovered
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"Document {_documentsCounters[index].DocId} contains a counter with name {_documentsCounters[index].Name} but we were not able to recover such counter.");
                        }

                        index++;
                    }
                }
            }
        }

        private void WriteDummyDocumentForTimeSeries(BlittableJsonTextWriter writer, string docId, IEnumerable<string> timeSeries)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Found orphan time series with document-Id '{docId}'");

            if (_documentWritten)
                writer.WriteComma();
            //start metadata
            writer.WriteStartObject();
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Key);
            writer.WriteStartObject();
            //collection name
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Collection);
            writer.WriteString(EmptyCollection);
            writer.WriteComma();
            //id
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Id);
            writer.WriteString(docId);
            writer.WriteComma();
            //change vector
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.ChangeVector);
            writer.WriteString(Empty);
            writer.WriteComma();
            //flags
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Flags);
            writer.WriteString(DocumentFlags.HasTimeSeries.ToString());
            writer.WriteComma();
            //start time-series
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.TimeSeries);
            //start counters array
            writer.WriteStartArray();
            var first = true;
            foreach (var ts in timeSeries)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Found orphan time-series with docId= {docId} and name={ts}.");

                writer.WriteString(ts);
            }

            // end time-series array
            writer.WriteEndArray();
            //end metadata
            writer.WriteEndObject();
            writer.WriteEndObject();

            _lastWriteIsDocument = true;
            _documentWritten = true;
        }

        private void WriteDummyDocumentForCounters(BlittableJsonTextWriter writer, string docId, List<string> counters)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Found orphan counter with document-Id '{docId}'");

            if (_documentWritten)
                writer.WriteComma();
            //start metadata
            writer.WriteStartObject();
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Key);
            writer.WriteStartObject();
            //collection name
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Collection);
            writer.WriteString(EmptyCollection);
            writer.WriteComma();
            //id
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Id);
            writer.WriteString(docId);
            writer.WriteComma();
            //change vector
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.ChangeVector);
            writer.WriteString(Empty);
            writer.WriteComma();
            //flags
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Flags);
            writer.WriteString(DocumentFlags.HasCounters.ToString());
            writer.WriteComma();
            //start counters
            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Counters);
            //start counters array
            writer.WriteStartArray();
            var first = true;
            foreach (var counter in counters)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Found orphan counter with docId= {docId} and name={counter}.");

                writer.WriteString(counter);
            }

            // end counters array
            writer.WriteEndArray();
            //end metadata
            writer.WriteEndObject();
            writer.WriteEndObject();

            _lastWriteIsDocument = true;
            _documentWritten = true;
        }

        private long _attachmentNumber = 0;
        private readonly List<(string hash, string tag, long size)> _attachmentsHashs = new List<(string, string, long)>();
        private const string TagPrefix = "Recovered attachment #";

        private void WriteAttachment(BlittableJsonTextWriter writer, long totalSize, string hash, string tag = null)
        {
            if (_documentWritten)
            {
                writer.WriteComma();
            }

            writer.WriteStartObject();

            writer.WritePropertyName(Raven.Client.Constants.Documents.Metadata.Key);
            writer.WriteStartObject();

            writer.WritePropertyName(DocumentItem.ExportDocumentType.Key);
            writer.WriteString(DocumentItem.ExportDocumentType.Attachment);

            writer.WriteEndObject();
            writer.WriteComma();

            writer.WritePropertyName(nameof(AttachmentName.Hash));
            writer.WriteString(hash);
            writer.WriteComma();

            writer.WritePropertyName(nameof(AttachmentName.Size));
            writer.WriteInteger(totalSize);
            writer.WriteComma();

            writer.WritePropertyName(nameof(DocumentItem.AttachmentStream.Tag));
            writer.WriteString(tag ?? $"{TagPrefix}{++_attachmentNumber}");

            writer.WriteEndObject();
            foreach (var chunk in _attachmentChunks)
            {
                writer.WriteMemoryChunk(chunk.Ptr, chunk.Size);
            }
            _attachmentsHashs.Add((hash, tag, totalSize));
            _lastWriteIsDocument = false;
            _lastAttachmentInfo = (hash, totalSize, tag);
            _documentWritten = true;
        }

        private bool ValidateOverflowPage(PageHeader* pageHeader, byte* eof, long startOffset, ref byte* mem)
        {
            ulong checksum;
            //pageHeader might be a buffer address we need to verify we don't exceed the original memory boundary here
            var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            var sizeOfPages = numberOfPages * _pageSize;
            var endOfOverflow = (long)mem + sizeOfPages;
            // the endOfOverflow can be equal to eof if the last page is overflow
            if (endOfOverflow > (long)eof)
            {
                var message =
                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                    $" size exceeds the end of the file ([{(long)mem}:{(long)endOfOverflow}])";
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
                checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, pageHeader->OverflowSize);

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

        private static void WriteSmugglerHeader(BlittableJsonTextWriter writer, int version, string docType)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("BuildVersion");
            writer.WriteInteger(version);
            writer.WriteComma();
            writer.WritePropertyName(docType);
            writer.WriteStartArray();
        }

        private bool Write(byte* mem, int sizeInBytes, BlittableJsonTextWriter documentsWriter, BlittableJsonTextWriter revisionsWriter,
            BlittableJsonTextWriter conflictsWriter, BlittableJsonTextWriter countersWriter, BlittableJsonTextWriter timeSeries, JsonOperationContext context, long startOffset, byte tableType)
        {
            switch ((TableType)tableType)
            {
                case TableType.None:
                    return false;

                case TableType.Documents:
                    return WriteDocument(mem, sizeInBytes, documentsWriter, context, startOffset);

                case TableType.Revisions:
                    return WriteRevision(mem, sizeInBytes, revisionsWriter, context, startOffset);

                case TableType.Conflicts:
                    return WriteConflict(mem, sizeInBytes, conflictsWriter, context, startOffset);

                case TableType.Counters:
                    return WriteCounter(mem, sizeInBytes, countersWriter, context, startOffset);

                case TableType.TimeSeries:
                    return WriteTimeSeries(mem, sizeInBytes, timeSeries, context, startOffset);

                default:
                    throw new ArgumentOutOfRangeException(nameof(tableType), tableType, null);
            }
        }

        private bool WriteTimeSeries(byte* mem, in int sizeInBytes, BlittableJsonTextWriter timeSeriesWriter, JsonOperationContext context, in long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                if (_timeSeriesWritten)
                    timeSeriesWriter.WriteComma();

                _timeSeriesWritten = false;

                TimeSeriesSegmentEntry item;
                try
                {
                    item = TimeSeriesStorage.CreateTimeSeriesItem(context, ref tvr);
                    if (item == null)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to convert table value to time-series at position {GetFilePosition(startOffset, mem)}");
                        return false;
                    }
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(
                            $"Found invalid time-series segment at position={GetFilePosition(startOffset, mem)}{Environment.NewLine}{e}");
                    return false;
                }

                timeSeriesWriter.WriteStartObject();
                {
                    timeSeriesWriter.WritePropertyName(Raven.Client.Constants.Documents.Blob.Document);

                    timeSeriesWriter.WriteStartObject();
                    {
                        timeSeriesWriter.WritePropertyName(nameof(TimeSeriesItem.DocId));
                        timeSeriesWriter.WriteString(item.DocId);
                        timeSeriesWriter.WriteComma();

                        timeSeriesWriter.WritePropertyName(nameof(TimeSeriesItem.Name));
                        timeSeriesWriter.WriteString(item.Name);
                        timeSeriesWriter.WriteComma();

                        timeSeriesWriter.WritePropertyName(nameof(TimeSeriesItem.ChangeVector));
                        timeSeriesWriter.WriteString(item.ChangeVector);
                        timeSeriesWriter.WriteComma();

                        timeSeriesWriter.WritePropertyName(nameof(TimeSeriesItem.Collection));
                        timeSeriesWriter.WriteString(item.Collection);
                        timeSeriesWriter.WriteComma();

                        timeSeriesWriter.WritePropertyName(nameof(TimeSeriesItem.Baseline));
                        timeSeriesWriter.WriteDateTime(item.Start, true);
                    }
                    timeSeriesWriter.WriteEndObject();

                    timeSeriesWriter.WriteComma();
                    timeSeriesWriter.WritePropertyName(Raven.Client.Constants.Documents.Blob.Size);
                    timeSeriesWriter.WriteInteger(item.SegmentSize);
                }
                timeSeriesWriter.WriteEndObject();

                timeSeriesWriter.WriteMemoryChunk(item.Segment.Ptr, item.Segment.NumberOfBytes);

                _timeSeriesWritten = true;
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found time-series segment with document Id={item.DocId} and time-series={item.Name}");

                _lastRecoveredDocumentKey = item.DocId;

                if (_uniqueTimeSeriesDiscovered.TryGetValue(item.DocId, out var hs) == false)
                {
                    _uniqueTimeSeriesDiscovered[item.DocId] = hs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                }

                hs.Add(item.Name);

                _numberOfTimeSeriesSegmentsRetrieved++;

                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing time-series segment at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private bool WriteCounter(byte* mem, int sizeInBytes, BlittableJsonTextWriter countersWriter, JsonOperationContext context, long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                if (_counterWritten)
                    countersWriter.WriteComma();

                _counterWritten = false;

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

                    CountersStorage.ConvertFromBlobToNumbers(context, counterGroup);
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Found invalid counter item at position={GetFilePosition(startOffset, mem)} with document Id={counterGroup?.DocumentId ?? "null"} and counter values={counterGroup?.Values}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(countersWriter, new DynamicJsonValue
                {
                    [nameof(CounterItem.DocId)] = counterGroup.DocumentId.ToString(),
                    [nameof(CounterItem.ChangeVector)] = counterGroup.ChangeVector.ToString(),
                    [nameof(CounterItem.Batch.Values)] = counterGroup.Values
                });

                _counterWritten = true;
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found counter item with document Id={counterGroup.DocumentId} and counter values={counterGroup.Values}");

                _lastRecoveredDocumentKey = counterGroup.DocumentId;

                if (counterGroup.Values.TryGet(CountersStorage.Values, out BlittableJsonReaderObject countersData) == false)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        using (var key = DocumentsStorage.TableValueToString(context, (int)CountersStorage.CountersTable.CounterKey, ref tvr))
                        {
                            _logger.Info(
                                $"Found counter-group item (key = '{key}') with counter-data document that is missing '{CountersStorage.Values}' property.");
                        }
                    }

                    return true;
                }

                var names = countersData.GetPropertyNames();

                if (_uniqueCountersDiscovered.TryGetValue(counterGroup.DocumentId, out var list) == false)
                {
                    _uniqueCountersDiscovered[counterGroup.DocumentId] = list = new List<string>();
                }
                list.AddRange(names);

                _numberOfCountersRetrieved += names.Length;

                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing counter item at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private bool WriteDocument(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, JsonOperationContext context, long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                if (_documentWritten)
                    writer.WriteComma();

                _documentWritten = false;

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
                    document.EnsureMetadata();
                    document.Data.BlittableValidation();

                    if (_previouslyWrittenDocs.TryGetValue(document.Id, out var previousEtag))
                    {
                        // This is a duplicate doc. It can happen when a page is marked as freed, but still exists in the data file.
                        // We determine which one to choose by their etag. If the document is newer, we will write it again to the
                        // smuggler file. This way, when importing, it will be the one chosen (last write wins)
                        if (document.Etag <= previousEtag)
                            return false;
                    }

                    _previouslyWrittenDocs[document.Id] = document.Etag;
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Found invalid blittable document at pos={GetFilePosition(startOffset, mem)} with key={document?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, document.Data);

                _documentWritten = true;
                _numberOfDocumentsRetrieved++;
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found document with key={document.Id}");
                _lastRecoveredDocumentKey = document.Id;

                HandleDocumentAttachments(document);
                HandleDocumentCounters(document);
                HandleDocumentTimeSeries(document);

                _lastWriteIsDocument = true;
                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing document at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private void HandleDocumentAttachments(Document document)
        {
            if (document.Flags.HasFlag(DocumentFlags.HasAttachments))
            {
                var metadata = document.Data.GetMetadata();
                if (metadata == null)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Document {document.Id} has attachment flag set but was unable to read its metadata and retrieve the attachments hashes");
                    return;
                }
                var metadataDictionary = new MetadataAsDictionary(metadata);
                var attachments = metadataDictionary.GetObjects(Raven.Client.Constants.Documents.Metadata.Attachments);
                foreach (var attachment in attachments)
                {
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    if (IsNullOrEmpty(hash))
                        continue;
                    _documentsAttachments.Add((hash, document.Id));
                }
            }
        }

        private void HandleDocumentCounters(Document document)
        {
            if (document.Flags.HasFlag(DocumentFlags.HasCounters))
            {
                var metadata = document.Data.GetMetadata();
                if (metadata == null)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Document {document.Id} has counters flag set but was unable to read its metadata and retrieve the counters names");
                    return;
                }
                metadata.TryGet(Raven.Client.Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters);
                foreach (var counter in counters)
                {
                    _documentsCounters.Add((counter.ToString(), document.Id));
                }
            }
        }

        private void HandleDocumentTimeSeries(Document document)
        {
            if (document.Flags.HasFlag(DocumentFlags.HasTimeSeries))
            {
                var metadata = document.Data.GetMetadata();
                if (metadata == null)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Document {document.Id} has time-series flag set but was unable to read its metadata and retrieve the time-series names");
                    return;
                }
                metadata.TryGet(Raven.Client.Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeries);
                foreach (var ts in timeSeries)
                {
                    _documentsTimeSeries.Add((ts.ToString(), document.Id));
                }
            }
        }

        private bool WriteRevision(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, JsonOperationContext context, long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                if (_revisionWritten)
                    writer.WriteComma();

                _revisionWritten = false;

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
                    revision.EnsureMetadata();
                    revision.Data.BlittableValidation();
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Found invalid blittable revision document at pos={GetFilePosition(startOffset, mem)} with key={revision?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, revision.Data);

                _revisionWritten = true;
                _numberOfDocumentsRetrieved++;
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found revision document with key={revision.Id}");
                _lastRecoveredDocumentKey = revision.Id;
                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing revision document at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private bool WriteConflict(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, JsonOperationContext context, long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                if (_conflictWritten)
                    writer.WriteComma();

                _conflictWritten = false;

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
                        _logger.Operations($"Found invalid blittable conflict document at pos={GetFilePosition(startOffset, mem)} with key={conflict?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, conflict.Doc);

                _conflictWritten = true;
                _numberOfDocumentsRetrieved++;
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found conflict document with key={conflict.Id}");
                _lastRecoveredDocumentKey = conflict.Id;
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
            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations(message);
            }
            _numberOfFaultedPages++;
            return mem + _pageSize;
        }

        private readonly string _output;
        private readonly int _pageSize;
        private AbstractPager Pager => _option.DataPager;
        private const string LogFileName = "recovery.log";
        private long _numberOfFaultedPages;
        private long _numberOfDocumentsRetrieved;
        private readonly int _initialContextSize;
        private readonly int _initialContextLongLivedSize;
        private bool _documentWritten;
        private bool _revisionWritten;
        private bool _conflictWritten;
        private bool _counterWritten;
        private bool _timeSeriesWritten;
        private StorageEnvironmentOptions _option;
        private readonly int _progressIntervalInSec;
        private bool _cancellationRequested;
        private string _lastRecoveredDocumentKey = "No documents recovered yet";
        private readonly string _datafile;
        private readonly bool _copyOnWrite;
        private readonly Dictionary<string, long> _previouslyWrittenDocs;
        private readonly List<(string Hash, string DocId)> _documentsAttachments = new List<(string Hash, string DocId)>();
        private readonly List<(string Name, string DocId)> _documentsCounters = new List<(string Name, string DocId)>();
        private readonly Dictionary<string, List<string>> _uniqueCountersDiscovered = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        private readonly List<(string Name, string DocId)> _documentsTimeSeries = new List<(string Name, string DocId)>();
        private readonly Dictionary<string, HashSet<string>> _uniqueTimeSeriesDiscovered = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        private long _numberOfCountersRetrieved;
        private long _numberOfTimeSeriesSegmentsRetrieved;
        private int _dummyDocNumber;
        private int _dummyAttachmentNumber;
        private bool _lastWriteIsDocument;
        private (string hash, long size, string tag)? _lastAttachmentInfo;
        private Logger _logger;
        private readonly byte[] _masterKey;
        private int _InvalidChecksumWithNoneZeroMac;
        private bool _shouldIgnoreInvalidPagesInARaw;
        private const int MaxNumberOfInvalidChecksumWithNoneZeroMac = 128;

        public bool IsEncrypted => _masterKey != null;

        public enum RecoveryStatus
        {
            Success,
            CancellationRequested
        }

        public void Dispose()
        {
            _option?.Dispose();
        }
    }
}

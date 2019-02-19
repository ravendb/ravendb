using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
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
            
            // by default CopyOnWriteMode will be true
            _copyOnWrite = !config.DisableCopyOnWriteMode;
            _config = config;
            _option = CreateOptions();
            
            _progressIntervalInSec = config.ProgressIntervalInSec;
            _previouslyWrittenDocs = new Dictionary<string, long>();
            if(config.LoggingMode != LogMode.None)
                LoggingSource.Instance.SetupLogMode(config.LoggingMode, Path.Combine(Path.GetDirectoryName(_output), LogFileName));
            _logger = LoggingSource.Instance.GetLogger<Recovery>("Voron Recovery");
        }

        private StorageEnvironmentOptions CreateOptions()
        {
            var result = StorageEnvironmentOptions.ForPath(_config.DataFileDirectory, null, null, null, null);
            result.CopyOnWriteMode = _copyOnWrite;
            result.ManualFlushing = true;
            result.ManualSyncing = true;
            result.IgnoreInvalidJournalErrors = _config.IgnoreInvalidJournalErrors;

            return result;
        }

        private readonly byte[] _streamHashState = new byte[(int)Sodium.crypto_generichash_statebytes()];
        private readonly byte[] _streamHashResult = new byte[(int)Sodium.crypto_generichash_bytes()];
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
            try
            {
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
                        {
                            try
                            {
                                se = new StorageEnvironment(_option);
                                break;
                            }
                            catch (IncreasingDataFileInCopyOnWriteModeException e)
                            {
                                _option.Dispose();

                                using (var file = File.Open(e.DataFilePath, FileMode.Open))
                                {
                                    file.SetLength(e.RequestedSize);
                                }

                                _option = CreateOptions();
                            }
                        }

                        mem = se.Options.DataPager.AcquirePagePointer(null, 0);
                        writer.WriteLine(
                            $"Journal recovery has completed successfully within {sw.Elapsed.TotalSeconds:N1} seconds");
                    }
                    catch (Exception e)
                    {
                        se?.Dispose();
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
                    mem = Pager.AcquirePagePointer(null, 0);
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
                using (var gZipStreamDocuments = new GZipStream(destinationStreamDocuments, CompressionMode.Compress, true))
                using (var gZipStreamRevisions = new GZipStream(destinationStreamRevisions, CompressionMode.Compress, true))
                using (var gZipStreamConflicts = new GZipStream(destinationStreamConflicts, CompressionMode.Compress, true))
                using (var gZipStreamCounters = new GZipStream(destinationStreamCounters, CompressionMode.Compress, true))
                using (var context = new JsonOperationContext(_initialContextSize, _initialContextLongLivedSize, SharedMultipleUseFlag.None))
                using (var documentsWriter = new BlittableJsonTextWriter(context, gZipStreamDocuments))
                using (var revisionsWriter = new BlittableJsonTextWriter(context, gZipStreamRevisions))
                using (var conflictsWriter = new BlittableJsonTextWriter(context, gZipStreamConflicts))
                using (var countersWriter = new BlittableJsonTextWriter(context, gZipStreamCounters))
                {
                    WriteSmugglerHeader(documentsWriter, ServerVersion.Build, "Docs");
                    WriteSmugglerHeader(revisionsWriter, ServerVersion.Build, nameof(DatabaseItemType.RevisionDocuments));
                    WriteSmugglerHeader(conflictsWriter, ServerVersion.Build, nameof(DatabaseItemType.Conflicts));
                    WriteSmugglerHeader(countersWriter, ServerVersion.Build, nameof(DatabaseItemType.CountersBatch));

                    while (mem < eof)
                    {
                        try
                        {
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

                            var pageHeader = (PageHeader*)mem;

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
                                                _logger.Operations($"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to initialize Sodium for hash computation will skip this page.");
                                            mem += numberOfPages * _pageSize;
                                            continue;
                                        }
                                        // write document header, including size
                                        PageHeader* nextPage = pageHeader;
                                        byte* nextPagePtr = (byte*)nextPage;
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
                                                    _logger.Operations($"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to compute chunk hash, will skip it.");
                                                valid = false;
                                                break;
                                            }
                                            if (streamPageHeader->StreamNextPageNumber == 0)
                                            {
                                                ExtractTagFromLastPage(nextPage, streamPageHeader, ref tag);
                                                break;
                                            }
                                            nextPage = (PageHeader*)(streamPageHeader->StreamNextPageNumber * _pageSize + startOffset);
                                            //This is the case that the next page isn't a stream page
                                            if (nextPage->Flags.HasFlag(PageFlags.Stream) == false || nextPage->Flags.HasFlag(PageFlags.Overflow) == false)
                                            {
                                                valid = false;
                                                if (_logger.IsOperationsEnabled)
                                                    _logger.Operations($"page #{nextPage->PageNumber} (offset={(long)nextPage}) was suppose to be a stream chunk but isn't marked as Overflow | Stream");
                                                break;
                                            }
                                            valid = ValidateOverflowPage(nextPage, eof, (long)nextPage, ref nextPagePtr);
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
                                                _logger.Operations($"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to compute attachment hash, will skip it.");
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

                                else if (Write((byte*)pageHeader + PageHeader.SizeOf, pageHeader->OverflowSize, documentsWriter, revisionsWriter,
                                    conflictsWriter, countersWriter, context, startOffset, ((RawDataOverflowPageHeader*)mem)->TableType))
                                {

                                    mem += numberOfPages * _pageSize;
                                }
                                else //write document failed 
                                {
                                    mem += _pageSize;
                                }
                                continue;
                            }

                            checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, 0);

                            if (checksum != pageHeader->Checksum)
                            {
                                var message =
                                    $"Invalid checksum for page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                                mem = PrintErrorAndAdvanceMem(message, mem);
                                continue;
                            }

                            // small raw data section 
                            var rawHeader = (RawDataSmallPageHeader*)mem;

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
                                var currMem = mem + pos;
                                var entry = (RawDataSection.RawDataEntrySizes*)currMem;
                                //this indicates that the current entry is invalid because it is outside the size of a page
                                if (pos > _pageSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, currMem)}";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }
                                //Allocated size of entry exceed the bound of the page next allocation
                                if (entry->AllocatedSize + pos + sizeof(RawDataSection.RawDataEntrySizes) >
                                    rawHeader->NextAllocation)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, currMem)}" +
                                        "the allocated entry exceed the bound of the page next allocation.";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }
                                if (entry->UsedSize > entry->AllocatedSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, currMem)}" +
                                        "the size of the entry exceed the allocated size";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }
                                pos += entry->AllocatedSize + sizeof(RawDataSection.RawDataEntrySizes);
                                if (entry->AllocatedSize == 0 || entry->UsedSize == -1)
                                    continue;

                                if (Write(currMem + sizeof(RawDataSection.RawDataEntrySizes), entry->UsedSize, documentsWriter, revisionsWriter,
                                        conflictsWriter, countersWriter, context, startOffset, ((RawDataSmallPageHeader*)mem)->TableType) == false)
                                    break;
                            }
                            mem += _pageSize;
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

                    documentsWriter.WriteEndArray();
                    conflictsWriter.WriteEndArray();
                    revisionsWriter.WriteEndArray();
                    countersWriter.WriteEndArray();
                    documentsWriter.WriteEndObject();
                    conflictsWriter.WriteEndObject();
                    revisionsWriter.WriteEndObject();
                    countersWriter.WriteEndObject();

                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations(Environment.NewLine +
                            $"Discovered a total of {_numberOfDocumentsRetrieved:#,#;00} documents within {sw.Elapsed.TotalSeconds::#,#.#;;00} seconds." + Environment.NewLine +
                            $"Discovered a total of {_attachmentsHashs.Count:#,#;00} attachments. " + Environment.NewLine +
                            $"Discovered a total of {_numberOfCountersRetrieved:#,#;00} counters. " + Environment.NewLine +
                            $"Discovered a total of {_numberOfFaultedPages::#,#;00} faulted pages.");
                    }

                }
                if (_cancellationRequested)
                    return RecoveryStatus.CancellationRequested;
                return RecoveryStatus.Success;
            }
            finally
            {
                se?.Dispose();
                if(_config.LoggingMode != LogMode.None)
                    LoggingSource.Instance.EndLogging();
            }
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
            _documentsAttachments.Sort((x, y) => Compare(x.hash, y.hash, StringComparison.Ordinal));
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
                    var documentHash = _documentsAttachments[index].hash;
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
                            _logger.Operations($"Document {_documentsAttachments[index].docId} contains attachment with hash {documentHash} but we were not able to recover such attachment.");
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

            var orphans = new Dictionary<string, HashSet<string>>();
            if (_documentsCounters.Count == 0)
            {
                foreach (var (name, docId) in _uniqueCountersDiscovered)
                {
                    AddOrphanCounter(orphans, docId, name);
                }

                ReportOrphanCountersDocumentIds(orphans, documentWriter);
                return;
            }
            writer.WriteLine("Starting to compute orphan and missing counters. this may take a while.");
            if (ct.IsCancellationRequested)
            {
                return;
            }
            _documentsCounters.Sort((x, y) => Compare(x.docId + SpecialChars.RecordSeparator + x.name,
                y.docId + SpecialChars.RecordSeparator + y.name, StringComparison.OrdinalIgnoreCase));
            //We rely on the fact that the counter id+name is unique in the _discoveredCounters list (no duplicated values).
            int index = 0;
            foreach (var (name, docId) in _uniqueCountersDiscovered)
            {
                var discoveredKey = docId + SpecialChars.RecordSeparator + name;
                if (ct.IsCancellationRequested)
                {
                    return;
                }
                var foundEqual = false;
                while (_documentsCounters.Count > index)
                {
                    var documentsCountersKey = _documentsCounters[index].docId + SpecialChars.RecordSeparator + _documentsCounters[index].name;
                    var compareResult = Compare(discoveredKey, documentsCountersKey, StringComparison.OrdinalIgnoreCase);
                    if (compareResult == 0)
                    {
                        index++;
                        foundEqual = true;
                        continue;
                    }
                    if (compareResult > 0)
                    {
                        //this is the case where we have a document with a counter that wasn't recovered
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Document {_documentsCounters[index].docId} contains a counter with name {_documentsCounters[index].name} but we were not able to recover such counter.");
                        index++;
                        continue;
                    }
                    break;
                }
                if (foundEqual == false)
                {
                    AddOrphanCounter(orphans, docId, name);
                }
            }

            if (orphans.Count > 0)
            {
                ReportOrphanCountersDocumentIds(orphans, documentWriter);
            }

        }

        private static void AddOrphanCounter(Dictionary<string, HashSet<string>> orphans, string docId, string name)
        {
            if (orphans.TryGetValue(docId, out var existing) == false)
            {
                orphans[docId] = new HashSet<string> { name };
            }
            else
            {
                existing.Add(name);
            }
        }

        private void ReportOrphanCountersDocumentIds(Dictionary<string, HashSet<string>> orphans, BlittableJsonTextWriter writer)
        {
            foreach (var kvp in orphans)
            {
                WriteDummyDocumentForCounters(writer, kvp.Key, kvp.Value);
            }
        }

        private void WriteDummyDocumentForCounters(BlittableJsonTextWriter writer, string docId, HashSet<string> counters)
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
            var endOfOverflow = (byte*)pageHeader + VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize) * _pageSize;
            // the endOfOverflow can be equal to eof if the last page is overflow
            if (endOfOverflow > eof)
            {
                var message =
                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                    $" size exceeds the end of the file ([{(long)pageHeader}:{(long)endOfOverflow}])";
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
            // this can only be here if we know that the overflow size is valid
            checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, pageHeader->OverflowSize);

            if (checksum != pageHeader->Checksum)
            {
                var message =
                    $"Invalid checksum for overflow page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                mem = PrintErrorAndAdvanceMem(message, mem);
                return false;
            }
            return true;
        }

        private void WriteSmugglerHeader(BlittableJsonTextWriter writer, int version, string docType)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("BuildVersion");
            writer.WriteInteger(version);
            writer.WriteComma();
            writer.WritePropertyName(docType);
            writer.WriteStartArray();
        }

        private bool Write(byte* mem, int sizeInBytes, BlittableJsonTextWriter documentsWriter, BlittableJsonTextWriter revisionsWriter,
            BlittableJsonTextWriter conflictsWriter, BlittableJsonTextWriter countersWriter, JsonOperationContext context, long startOffset, byte tableType)
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
                default:
                    throw new ArgumentOutOfRangeException(nameof(tableType), tableType, null);
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
                    counterGroup = CountersStorage.TableValueToCounterGroupDetail(context, tvr);
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
                        _logger.Operations($"Found invalid counter item at position={GetFilePosition(startOffset, mem)} with document Id={counterGroup?.CounterKey ?? "null"} and counter values={counterGroup?.Values}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(countersWriter, new DynamicJsonValue
                {
                    [nameof(CounterItem.DocId)] = counterGroup.CounterKey.ToString(),
                    [nameof(CounterItem.ChangeVector)] = counterGroup.ChangeVector.ToString(),
                    [nameof(CounterItem.Batch.Values)] = counterGroup.Values
                });

                _counterWritten = true;
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Found counter item with document Id={counterGroup.CounterKey} and counter values={counterGroup.Values}");

                _lastRecoveredDocumentKey = counterGroup.CounterKey;
                _uniqueCountersDiscovered.Add((null, counterGroup.CounterKey));
                _numberOfCountersRetrieved++;

                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing counter item at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private bool WriteDocument(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, JsonOperationContext context, long startOffest)
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
                            _logger.Operations($"Failed to convert table value to document at position {GetFilePosition(startOffest, mem)}");
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
                        _logger.Operations($"Found invalid blittable document at pos={GetFilePosition(startOffest, mem)} with key={document?.Id ?? "null"}{Environment.NewLine}{e}");
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

                _lastWriteIsDocument = true;
                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing document at position {GetFilePosition(startOffest, mem)}: {e}");
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

        private bool WriteRevision(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, JsonOperationContext context, long startOffest)
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
                            _logger.Operations($"Failed to convert table value to revision document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    revision.EnsureMetadata();
                    revision.Data.BlittableValidation();
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Found invalid blittable revision document at pos={GetFilePosition(startOffest, mem)} with key={revision?.Id ?? "null"}{Environment.NewLine}{e}");
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
                    _logger.Operations($"Unexpected exception while writing revision document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        private bool WriteConflict(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, JsonOperationContext context, long startOffest)
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
                            _logger.Operations($"Failed to convert table value to conflict document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    conflict.Doc.BlittableValidation();
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Found invalid blittable conflict document at pos={GetFilePosition(startOffest, mem)} with key={conflict?.Id ?? "null"}{Environment.NewLine}{e}");
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
                    _logger.Operations($"Unexpected exception while writing conflict document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* PrintErrorAndAdvanceMem(string message, byte* mem)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations(message);
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
        private StorageEnvironmentOptions _option;
        private readonly int _progressIntervalInSec;
        private bool _cancellationRequested;
        private string _lastRecoveredDocumentKey = "No documents recovered yet";
        private readonly string _datafile;
        private readonly bool _copyOnWrite;
        private readonly Dictionary<string, long> _previouslyWrittenDocs;
        private readonly List<(string hash, string docId)> _documentsAttachments = new List<(string hash, string docId)>();
        private readonly List<(string name, string docId)> _documentsCounters = new List<(string name, string docId)>();
        private readonly SortedSet<(string name, string docId)> _uniqueCountersDiscovered = new SortedSet<(string name, string docId)>(new ByDocIdAndCounterName());

        private long _numberOfCountersRetrieved;
        private int _dummyDocNumber;
        private int _dummyAttachmentNumber;
        private bool _lastWriteIsDocument;
        private (string hash, long size, string tag)? _lastAttachmentInfo;
        private Logger _logger;

        public enum RecoveryStatus
        {
            Success,
            CancellationRequested
        }

        private class ByDocIdAndCounterName : IComparer<(string name, string docId)>
        {
            public int Compare((string name, string docId) x, (string name, string docId) y)
            {
                return CaseInsensitiveComparer.Default.Compare(x.docId + SpecialChars.RecordSeparator + x.name,
                    y.docId + SpecialChars.RecordSeparator + y.name);
            }
        }

        public void Dispose()
        {
            _option?.Dispose();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Sparrow.Json;
using Sparrow.Threading;
using Voron.Data;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Recovery
{
    public unsafe class Recovery
    {
        public Recovery(VoronRecoveryConfiguration config)
        {
            _datafile = config.PathToDataFile;
            _output = config.OutputFileName;
            _pageSize = config.PageSizeInKb * Constants.Size.Kilobyte;
            _initialContextSize = config.InitialContextSizeInMB * Constants.Size.Megabyte;
            _initialContextLongLivedSize = config.InitialContextLongLivedSizeInKB * Constants.Size.Kilobyte;
            _option = StorageEnvironmentOptions.ForPath(config.DataFileDirectory, null, Path.Combine(config.DataFileDirectory, "Journal"), null, null);
            _copyOnWrite = !config.DisableCopyOnWriteMode;
            // by default CopyOnWriteMode will be true
            _option.CopyOnWriteMode = _copyOnWrite;
            _progressIntervalInSec = config.ProgressIntervalInSec;
            _previouslyWrittenDocs = new Dictionary<string, long>();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetFilePosition(long offset, byte* position)
        {
            return (long)position - offset;
        }

        public RecoveryStatus Execute(CancellationToken ct)
        {
            var sw = new Stopwatch();
            StorageEnvironment se = null;
            sw.Start();
            if (_copyOnWrite)
            {
                Console.WriteLine("Recovering journal files, this may take a while...");
                try
                {

                    se = new StorageEnvironment(_option);
                    Console.WriteLine(
                        $"Journal recovery has completed successfully within {sw.Elapsed.TotalSeconds:N1} seconds");
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Journal recovery failed, reason:{Environment.NewLine}{e}");
                }
                finally
                {
                    se?.Dispose();
                }
            }
            _option = StorageEnvironmentOptions.ForPath(Path.GetDirectoryName(_datafile));

            var mem = Pager.AcquirePagePointer(null, 0);
            long startOffset = (long)mem;
            var fi = new FileInfo(_datafile);
            var fileSize = fi.Length;
            //making sure eof is page aligned
            var eof = mem + (fileSize / _pageSize) * _pageSize;
            DateTime lastProgressReport = DateTime.MinValue;

            using (var destinationStreamDocuments = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-2-Documents" + Path.GetExtension(_output))))
            using (var destinationStreamRevisions = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-3-Revisions" + Path.GetExtension(_output))))
            using (var destinationStreamConflicts = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-4-Conflicts" + Path.GetExtension(_output))))
            using (var logFile = File.CreateText(Path.Combine(Path.GetDirectoryName(_output), LogFileName)))
            using (var gZipStreamDocuments = new GZipStream(destinationStreamDocuments, CompressionMode.Compress, true))
            using (var gZipStreamRevisions = new GZipStream(destinationStreamRevisions, CompressionMode.Compress, true))
            using (var gZipStreamConflicts = new GZipStream(destinationStreamConflicts, CompressionMode.Compress, true))
            using (var context = new JsonOperationContext(_initialContextSize, _initialContextLongLivedSize, SharedMultipleUseFlag.None))
            using (var documentsWriter = new BlittableJsonTextWriter(context, gZipStreamDocuments))
            using (var revisionsWriter = new BlittableJsonTextWriter(context, gZipStreamRevisions))
            using (var conflictsWriter = new BlittableJsonTextWriter(context, gZipStreamConflicts))
            {
                WriteSmugglerHeader(documentsWriter, 40018, "Docs");
                WriteSmugglerHeader(revisionsWriter, 40018, "RevisionDocuments");
                WriteSmugglerHeader(conflictsWriter, 40018, "ConflictDocuments");

                while (mem < eof)
                {
                    try
                    {
                        if (ct.IsCancellationRequested)
                        {
                            logFile.WriteLine(
                                $"Cancellation requested while recovery was in position {GetFilePosition(startOffset, mem)}");
                            _cancellationRequested = true;
                            break;
                        }
                        var now = DateTime.UtcNow;
                        if ((now - lastProgressReport).TotalSeconds >= _progressIntervalInSec)
                        {
                            if (lastProgressReport != DateTime.MinValue)
                            {
                                Console.Clear();
                                Console.WriteLine("Press 'q' to quit the recovery process");
                            }
                            lastProgressReport = now;
                            var currPos = GetFilePosition(startOffset, mem);
                            var eofPos = GetFilePosition(startOffset, eof);
                            Console.WriteLine(
                                $"{now:hh:MM:ss}: Recovering page at position {currPos:#,#;;0}/{eofPos:#,#;;0} ({(double)currPos / eofPos:p}) - Last recovered doc is {_lastRecoveredDocumentKey}");
                        }

                        var pageHeader = (PageHeader*)mem;
                        
                        //this page is not raw data section move on
                        if ((pageHeader->Flags).HasFlag(PageFlags.RawData) == false)
                        {
                            mem += _pageSize;
                            continue;
                        }

                        if (pageHeader->Flags.HasFlag(PageFlags.Single) &&
                            pageHeader->Flags.HasFlag(PageFlags.Overflow))
                        {
                            var message =
                                $"page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)}) has both Overflow and Single flag turned";
                            mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                            continue;
                        }
                        //overflow page
                        ulong checksum;
                        if (pageHeader->Flags.HasFlag(PageFlags.Overflow))
                        {
                            var endOfOverflow = (byte*)pageHeader + VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize) * _pageSize;
                            // the endOfOeverFlow can be equal to eof if the last page is overflow
                            if (endOfOverflow > eof)
                            {
                                var message =
                                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                                    $" size exceeds the end of the file ([{(long)pageHeader}:{(long)endOfOverflow}])";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                continue;
                            }

                            if (pageHeader->OverflowSize <= 0)
                            {
                                var message =
                                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                                    $" OverflowSize is not a positive number ({pageHeader->OverflowSize})";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                continue;
                            }
                            // this can only be here if we know that the overflow size is valid
                            checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, pageHeader->OverflowSize);

                            if (checksum != pageHeader->Checksum)
                            {
                                var message =
                                    $"Invalid checksum for overflow page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                continue;
                            }


                            if (Write((byte*)pageHeader + PageHeader.SizeOf, pageHeader->OverflowSize, documentsWriter, revisionsWriter, 
                                conflictsWriter, logFile, context, startOffset, ((RawDataOverflowPageHeader*)mem)->TableType))
                            {
                                var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);
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
                            mem = PrintErrorAndAdvanceMem(message, mem, logFile);
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
                            mem = PrintErrorAndAdvanceMem(message, mem, logFile);
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
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                //we can't retrive entries past the invalid entry
                                break;
                            }
                            //Allocated size of entry exceed the bound of the page next allocation
                            if (entry->AllocatedSize + pos + sizeof(RawDataSection.RawDataEntrySizes) >
                                rawHeader->NextAllocation)
                            {
                                var message =
                                    $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, currMem)}" +
                                    "the allocated entry exceed the bound of the page next allocation.";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                //we can't retrive entries past the invalid entry
                                break;
                            }
                            if (entry->UsedSize > entry->AllocatedSize)
                            {
                                var message =
                                    $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, currMem)}" +
                                    "the size of the entry exceed the allocated size";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                //we can't retrive entries past the invalid entry
                                break;
                            }
                            pos += entry->AllocatedSize + sizeof(RawDataSection.RawDataEntrySizes);
                            if (entry->AllocatedSize == 0 || entry->UsedSize == -1)
                                continue;

                            if (Write(currMem + sizeof(RawDataSection.RawDataEntrySizes), entry->UsedSize, documentsWriter, revisionsWriter, 
                                conflictsWriter, logFile, context, startOffset, ((RawDataSmallPageHeader*)mem)->TableType) == false)
                                break;
                        }
                        mem += _pageSize;
                    }
                    catch (Exception e)
                    {
                        var message =
                            $"Unexpected exception at position {GetFilePosition(startOffset, mem)}:{Environment.NewLine} {e}";
                        mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                    }
                }
                documentsWriter.WriteEndArray();
                conflictsWriter.WriteEndArray();
                revisionsWriter.WriteEndArray();
                documentsWriter.WriteEndObject();
                conflictsWriter.WriteEndObject();
                revisionsWriter.WriteEndObject();

                logFile.WriteLine(
                    $"Discovered a total of {_numberOfDocumentsRetrieved:#,#;00} documents within {sw.Elapsed.TotalSeconds::#,#.#;;00} seconds.");
                logFile.WriteLine($"Discovered a total of {_numberOfFaultedPages::#,#;00} faulted pages.");
            }
            if (_cancellationRequested)
                return RecoveryStatus.CancellationRequested;
            return RecoveryStatus.Success;
        }

        private void WriteSmugglerHeader(BlittableJsonTextWriter writer, int version,string docType)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(("BuildVersion"));
            writer.WriteInteger(version);
            writer.WriteComma();
            writer.WritePropertyName(docType);
            writer.WriteStartArray();
        }

        private bool Write(byte* mem, int sizeInBytes, BlittableJsonTextWriter documentsWriter, BlittableJsonTextWriter revisionsWriter, 
            BlittableJsonTextWriter conflictsWritet, StreamWriter logWriter, JsonOperationContext context, long startOffest, byte tableType)
        {
            switch ((TableType)tableType)
            {
                case TableType.None:
                    return false;
                case TableType.Documents:
                    return WriteDocument(mem, sizeInBytes, documentsWriter, logWriter, context, startOffest);
                case TableType.Revisions:
                    return WriteRevision(mem, sizeInBytes, revisionsWriter, logWriter, context, startOffest);
                case TableType.Conflicts:
                    return WriteConflict(mem, sizeInBytes, conflictsWritet, logWriter, context, startOffest);
                default:
                    throw new ArgumentOutOfRangeException(nameof(tableType), tableType, null);
            }
        }

        private bool WriteDocument(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, StreamWriter logWriter, JsonOperationContext context, long startOffest)
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
                    document = DocumentsStorage.ParseRawDataSectionDocumentWithValidation(context, ref tvr, sizeInBytes, out var currentEtag);
                    if (document == null)
                    {
                        logWriter.WriteLine($"Failed to convert table value to document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    document.EnsureMetadata();
                    document.Data.BlittableValidation();

                    if (_previouslyWrittenDocs.TryGetValue(document.Id, out var previousEtag))
                    {
                        // This is a duplicate doc. It can happen when a page is marked as freed, but still exists in the data file.
                        // We determine which one to choose by their etag. If the document is newer, we will write it again to the
                        // smuggler file. This way, when importing, it will be the one chosen (last write wins)
                        if (currentEtag <= previousEtag)
                            return false;
                    }

                    _previouslyWrittenDocs[document.Id] = currentEtag;
                }
                catch (Exception e)
                {
                    logWriter.WriteLine(
                        $"Found invalid blittable document at pos={GetFilePosition(startOffest, mem)} with key={document?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, document.Data);

                _documentWritten = true;
                _numberOfDocumentsRetrieved++;
                logWriter.WriteLine($"Found document with key={document.Id}");
                _lastRecoveredDocumentKey = document.Id;
                return true;
            }
            catch (Exception e)
            {
                logWriter.WriteLine($"Unexpected exception while writing document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        private bool WriteRevision(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, StreamWriter logWriter, JsonOperationContext context, long startOffest)
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
                        logWriter.WriteLine($"Failed to convert table value to revision document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    revision.EnsureMetadata();
                    revision.Data.BlittableValidation();
                }
                catch (Exception e)
                {
                    logWriter.WriteLine(
                        $"Found invalid blittable revision document at pos={GetFilePosition(startOffest, mem)} with key={revision?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, revision.Data);

                _revisionWritten = true;
                _numberOfDocumentsRetrieved++;
                logWriter.WriteLine($"Found revision document with key={revision.Id}");
                _lastRecoveredDocumentKey = revision.Id;
                return true;
            }
            catch (Exception e)
            {
                logWriter.WriteLine($"Unexpected exception while writing revision document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        private bool WriteConflict(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, StreamWriter logWriter, JsonOperationContext context, long startOffest)
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
                        logWriter.WriteLine($"Failed to convert table value to conflict document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    conflict.Doc.BlittableValidation();
                }
                catch (Exception e)
                {
                    logWriter.WriteLine(
                        $"Found invalid blittable conflict document at pos={GetFilePosition(startOffest, mem)} with key={conflict?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, conflict.Doc);

                _conflictWritten = true;
                _numberOfDocumentsRetrieved++;
                logWriter.WriteLine($"Found conflict document with key={conflict.Id}");
                _lastRecoveredDocumentKey = conflict.Id;
                return true;
            }
            catch (Exception e)
            {
                logWriter.WriteLine($"Unexpected exception while writing conflict document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* PrintErrorAndAdvanceMem(string message, byte* mem, StreamWriter writer)
        {
            writer.WriteLine(message);
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
        private StorageEnvironmentOptions _option;
        private readonly int _progressIntervalInSec;
        private bool _cancellationRequested;
        private string _lastRecoveredDocumentKey = "No documents recovered yet";
        private readonly string _datafile;
        private readonly bool _copyOnWrite;
        private readonly Dictionary<string, long> _previouslyWrittenDocs;
        
        public enum RecoveryStatus
        {
            Success,
            CancellationRequested
        }
    }
}

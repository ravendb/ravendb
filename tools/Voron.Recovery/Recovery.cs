using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Server.Documents;
using Sparrow.Json;
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
            _pageSize = config.PageSizeInKb*Constants.Size.Kilobyte;
            _numberOfFieldsInDocumentTable = config.NumberOfFiledsInDocumentTable;
            _initialContextSize = config.InitialContextSizeInMB * Constants.Size.Megabyte;
            _initialContextLongLivedSize = config.InitialContextLongLivedSizeInKB*Constants.Size.Kilobyte;
            _option = StorageEnvironmentOptions.ForPath(config.DataFileDirectory);
            _copyOnWrite = !config.DisableCopyOnWriteMode;
            // by default CopyOnWriteMode will be true
            //i'm setting CopyOnWriteMode this was because we want to keep it internal.
            _option.GetType().GetProperty("CopyOnWriteMode", BindingFlags.NonPublic|BindingFlags.Instance).SetValue(_option, _copyOnWrite);
            _progressIntervalInSeconds = config.ProgressIntervalInSeconds;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetFilePosition(long offset, byte* position)
        {
            return (long)position-offset;
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
                    Console.WriteLine($"Journal recovery has completed successfully within {sw.Elapsed.TotalSeconds:N1} seconds");                    
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Journal recovery failed, reason:{Environment.NewLine}{e}");
                    // The reason i create a new storage enviroment option is because the old one is using copy on write
                    // And it may have wrote junk to the datafile and i need to open a "clean" MMF of the data file.
                    _option = StorageEnvironmentOptions.ForPath(Path.GetDirectoryName(_datafile));
                }
            }
            //Since we use StorageEnviroment data pager we can't dispose of it before we are done with the data pager.
            try
            {
                var mem = Pager.AcquirePagePointer(null, 0);
                long startOffest = (long) mem;
                var fi = new FileInfo(_datafile);
                var fileSize = fi.Length;
                //making sure eof is page aligned
                var eof = mem + (fileSize/_pageSize)*_pageSize;
                DateTime lastProgressReport = DateTime.MinValue;
                using (var destinationStream = File.OpenWrite(_output))
                using (var logFile = File.CreateText(Path.Combine(Path.GetDirectoryName(_output), LogFileName)))
                using (var gZipStream = new GZipStream(destinationStream, CompressionMode.Compress, true))
                using (var context = new JsonOperationContext(_initialContextSize, _initialContextLongLivedSize))
                using (var writer = new BlittableJsonTextWriter(context, gZipStream))
                {
                    WriteSmugglerHeader(writer);
                    while (mem < eof)
                    {
                        try
                        {
                            if (ct.IsCancellationRequested)
                            {
                                logFile.WriteLine(
                                    $"Cancellation requested while recovery was in position {GetFilePosition(startOffest, mem)}");
                                _cancellationRequested = true;
                                break;
                            }
                            var now = DateTime.UtcNow;
                            if ((now - lastProgressReport).TotalSeconds >= _progressIntervalInSeconds)
                            {
                                if (lastProgressReport != DateTime.MinValue)
                                {
                                    Console.Clear();
                                    Console.WriteLine("Press 'q' to quit the recovery process");
                                }
                                lastProgressReport = now;
                                var currPos = GetFilePosition(startOffest, mem);
                                var eofPos = GetFilePosition(startOffest, eof);
                                Console.WriteLine(
                                    $"{now:hh:MM:ss}: Recovering page at position {currPos:#,#;;0}/{eofPos:#,#;;0} ({(double) currPos/eofPos:p}) - Last recovered doc is {_lastRecoveredDocumentKey}");
                            }
                            var pageHeader = (PageHeader*) mem;
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
                                    $"page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffest, mem)}) has both Overflow and Single flag turned";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                continue;
                            }
                            //overflow page
                            if (pageHeader->Flags.HasFlag(PageFlags.Overflow))
                            {

                                var endOfOverflow = pageHeader +
                                                    Pager.GetNumberOfOverflowPages(pageHeader->OverflowSize)*_pageSize;
                                // the endOfOeverFlow can be equal to eof if the last page is overflow
                                if (endOfOverflow > eof)
                                {
                                    var message =
                                        $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffest, mem)})" +
                                        $" size exceeds the end of the file ([{(long) pageHeader}:{(long) endOfOverflow}])";
                                    mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                    continue;
                                }
                                //Should we increase the check size to page size (0=>_pageSize)?
                                if (pageHeader->OverflowSize <= 0)
                                {
                                    var message =
                                        $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffest, mem)})" +
                                        $" OverflowSize is not a positive number ({pageHeader->OverflowSize})";
                                    mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                    continue;
                                }
                                if (WriteDocument((byte*) pageHeader + sizeof(PageHeader), pageHeader->OverflowSize,
                                    writer,
                                    logFile, context, startOffest))
                                {
                                    var numberOfPages = Pager.GetNumberOfOverflowPages(pageHeader->OverflowSize);
                                    mem += numberOfPages*_pageSize;
                                }
                                else
                                    //write document failed 
                                {
                                    mem += _pageSize;
                                }
                                continue;
                            }
                            // small raw data section
                            var rawHeader = (RawDataSmallPageHeader*) mem;
                            if (rawHeader->RawDataFlags.HasFlag(RawDataPageFlags.Header))
                            {
                                mem += _pageSize;
                                continue;
                            }
                            if (rawHeader->NextAllocation > _pageSize)
                            {
                                var message =
                                    $"RawDataSmallPage #{rawHeader->PageNumber} at {GetFilePosition(startOffest, mem)} next allocation is larger than {_pageSize} bytes";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                continue;
                            }

                            for (var pos = sizeof(PageHeader); pos < rawHeader->NextAllocation;)
                            {
                                var currMem = mem + pos;
                                var entry = (RawDataSection.RawDataEntrySizes*) currMem;
                                //this indicates that the current entry is invalid because it is outside the size of a page
                                if (pos > _pageSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffest, currMem)}";
                                    mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                    //we can't retrive entries past the invalid entry
                                    break;
                                }
                                //Allocated size of entry exceed the bound of the page next allocation
                                if (entry->AllocatedSize + pos + sizeof(RawDataSection.RawDataEntrySizes) >
                                    rawHeader->NextAllocation)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffest, currMem)}" +
                                        "the allocated entry exceed the bound of the page next allocation.";
                                    mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                    //we can't retrive entries past the invalid entry
                                    break;
                                }
                                if (entry->UsedSize > entry->AllocatedSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffest, currMem)}" +
                                        "the size of the entry exceed the allocated size";
                                    mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                    //we can't retrive entries past the invalid entry
                                    break;
                                }
                                pos += entry->AllocatedSize + sizeof(RawDataSection.RawDataEntrySizes);
                                if (entry->AllocatedSize == 0 || entry->UsedSize == -1)
                                    continue;
                                if (
                                    WriteDocument(currMem + sizeof(RawDataSection.RawDataEntrySizes), entry->UsedSize,
                                        writer, logFile, context, startOffest) == false)
                                    break;
                            }
                            mem += _pageSize;
                        }
                        catch (Exception e)
                        {
                            var message =
                                $"Unexpected exception at position {GetFilePosition(startOffest, mem)}:{Environment.NewLine} {e}";
                            mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                        }
                    }
                    writer.WriteEndArray();
                    writer.WriteEndObject();
                    logFile.WriteLine(
                        $"Discovered a total of {_numberOfDocumentsRetrived:#,#;00} documents within {sw.Elapsed.TotalSeconds::#,#.#;;00} seconds.");
                    logFile.WriteLine($"Discovered a total of {_numberOfFaultedPages::#,#;00} faulted pages.");
                }
                if (_cancellationRequested)
                    return RecoveryStatus.CancellationRequested;
            }
            finally
            {
                se?.Dispose();
            }
            return RecoveryStatus.Success;
        }

        private void WriteSmugglerHeader(BlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(("BuildVersion"));
            writer.WriteInteger(40000);
            writer.WriteComma();
            writer.WritePropertyName(("Docs"));
            writer.WriteStartArray();
        }

        private bool WriteDocument(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, StreamWriter logWriter, JsonOperationContext context,long startOffest)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);
                if (tvr.Count != _numberOfFieldsInDocumentTable)
                {
                    var message =
                        $"Failed to read document at position {GetFilePosition(startOffest,mem)} because the TableValueReader number of entries" +
                        $" doesn't match NumberOfFiledsInDocumentTable={_numberOfFieldsInDocumentTable}";
                    //we actually not advancing the memory here because we might write a small data section entry
                    PrintErrorAndAdvanceMem(message, mem, logWriter);
                    return false;
                }

                if (_firstDoc == false)
                    writer.WriteComma();
                
                _firstDoc = false;
                Document document = null;              
                try
                {
                    document = DocumentsStorage.TableValueToDocument(context, tvr);
                    if (document == null)
                    {
                        logWriter.WriteLine(
                            $"Failed to convert table value to document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    document.EnsureMetadata();
                    document.Data.BlittableValidation();
                }
                catch (Exception e)
                {
                    logWriter.WriteLine(
                        $"Found invalid blittable document at pos={GetFilePosition(startOffest, mem)} with key={document?.Key ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }
                context.Write(writer, document.Data);
                _numberOfDocumentsRetrived++;
                logWriter.WriteLine($"Found Document with key={document.Key}");
                _lastRecoveredDocumentKey = document.Key;
                return true;
            }
            catch (Exception e)
            {
                logWriter.WriteLine($"Unexpected exception while writing document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* PrintErrorAndAdvanceMem(string message, byte* mem,StreamWriter writer)
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
        private long _numberOfDocumentsRetrived;
        private readonly int _numberOfFieldsInDocumentTable;
        private readonly int _initialContextSize;
        private readonly int _initialContextLongLivedSize;
        private bool _firstDoc = true;
        private StorageEnvironmentOptions _option;
        private readonly int _progressIntervalInSeconds;
        private bool _cancellationRequested;
        private string _lastRecoveredDocumentKey = "No documents recovered yet";
        private readonly string _datafile;
        private bool _copyOnWrite;


        public enum RecoveryStatus
        {
            Success,
            CancellationRequested
        }
    }
}

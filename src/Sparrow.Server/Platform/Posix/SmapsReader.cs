using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Sparrow.Platform.Posix
{
    internal class SmapsReaderResults
    {
        public string ResultString;
        public long Size;
        public long Rss;
        public long SharedClean;
        public long SharedDirty;
        public long PrivateClean;
        public long PrivateDirty;
    }
    
    internal interface ISmapsReaderResultAction
    {
        void Add(SmapsReaderResults results);
    }

    internal struct SmapsReaderJsonResults : ISmapsReaderResultAction
    {
        private DynamicJsonArray _dja;

        public void Add(SmapsReaderResults results)
        {
            var djv = new DynamicJsonValue
            {
                ["File"] = results.ResultString,
                ["Size"] = Sizes.Humane(results.Size),
                ["Rss"] = Sizes.Humane(results.Rss),
                ["SharedClean"] = Sizes.Humane(results.SharedClean),
                ["SharedDirty"] = Sizes.Humane(results.SharedDirty),
                ["PrivateClean"] = Sizes.Humane(results.PrivateClean),
                ["PrivateDirty"] = Sizes.Humane(results.PrivateDirty),
                ["TotalClean"] = results.SharedClean + results.PrivateClean,
                ["TotalCleanHumanly"] = Sizes.Humane(results.SharedClean + results.PrivateClean),
                ["TotalDirty"] = results.SharedDirty + results.PrivateDirty,
                ["TotalDirtyHumanly"] = Sizes.Humane(results.SharedDirty + results.PrivateDirty)
            };
            if (_dja == null)
                _dja = new DynamicJsonArray();
            _dja.Add(djv);
        }

        public DynamicJsonArray ReturnResults()
        {
            return _dja;
        }
    }
    
    internal struct SmapsReaderNoAllocResults : ISmapsReaderResultAction
    {
        public void Add(SmapsReaderResults results)
        {
        // currently we do not use these results with SmapsReaderNoAllocResults so we do not store them
        }
    }
    
    internal class SmapsReader
    {
        // this /proc/self/smaps reader assumes the format of smaps will always be with the following order:
        // - filename line (where we count rw-s) where with white-spaces delimeters - rw-s is second word in line and filename is last word
        // after that, in the following order :
        // Size, Rss, Shared_Clean, Private_Clean, Shared_Dirty, Private_Dirty and in order to finish reading a file data : Locked
        // Each must have with white-space delimiters a value, delimiter, "kB"

        public const int BufferSize = 4096;
        private readonly byte[][] _smapsBuffer;
        private readonly SmapsReaderResults _smapsReaderResults = new SmapsReaderResults();

        private readonly byte[] _rwsBytes = Encoding.UTF8.GetBytes("rw-s");
        private readonly byte[] _sizeBytes = Encoding.UTF8.GetBytes("Size:");
        private readonly byte[] _rssBytes = Encoding.UTF8.GetBytes("Rss:");
        private readonly byte[] _sharedCleanBytes = Encoding.UTF8.GetBytes("Shared_Clean:");
        private readonly byte[] _sharedDirtyBytes = Encoding.UTF8.GetBytes("Shared_Dirty:");
        private readonly byte[] _privateCleanBytes = Encoding.UTF8.GetBytes("Private_Clean:");
        private readonly byte[] _privateDirtyBytes = Encoding.UTF8.GetBytes("Private_Dirty:");

        private readonly byte[] _lockedBytes = Encoding.UTF8.GetBytes("Locked:");
        private readonly byte[] _tempBufferBytes = new byte[256];

        private readonly int[] _endOfBuffer = {0, 0};
        private int _currentBuffer;

        private enum SearchState
        {
            None,
            Rws,
            Size,
            Rss,
            SharedClean,
            SharedDirty,
            PrivateClean,
            PrivateDirty
        }

        public SmapsReader(byte[][] smapsBuffer)
        {
            _smapsBuffer = smapsBuffer;
        }
        
        private int ReadFromFile(Stream fileStream, int bufferIndex)
        {
            var read = fileStream.Read(_smapsBuffer[bufferIndex], 0, _smapsBuffer[bufferIndex].Length);
            _endOfBuffer[bufferIndex] = read;
            return read;
        }

        public (long Rss, long SharedClean, long PrivateClean, T SmapsResults) CalculateMemUsageFromSmaps<T>() where T : struct, ISmapsReaderResultAction
        {
            _endOfBuffer[0] = 0;
            _endOfBuffer[1] = 0;
            _currentBuffer = 0;
            
            var state = SearchState.None;
            var smapResultsObject = new T();

            using (var currentProcess = Process.GetCurrentProcess())
            using (var fileStream = new FileStream($"/proc/{currentProcess.Id}/smaps", FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var read = ReadFromFile(fileStream, _currentBuffer);
                var offsetForNextBuffer = 0;
                long tmpRss = 0, tmpSharedClean = 0, tmpPrivateClean = 0;
                string resultString = null;
                long valSize = 0, valRss = 0, valPrivateDirty = 0, valSharedDirty = 0, valSharedClean = 0, valPrivateClean = 0;
                while (true)
                {
                    if (read == 0)
                    {
                        return (tmpRss, tmpSharedClean, tmpPrivateClean, smapResultsObject);
                    }

                    var switchBuffer = false;
                    for (var i = offsetForNextBuffer; i < _endOfBuffer[_currentBuffer]; i++)
                    {
                        byte[] term;
                        var offset = 0;
                        if (_smapsBuffer[_currentBuffer][i] == 'r')
                            term = _rwsBytes;
                        else if (_smapsBuffer[_currentBuffer][i] == 'R')
                            term = _rssBytes;
                        else if (_smapsBuffer[_currentBuffer][i] == 'S')
                        {
                            term = _sizeBytes; // or SharedDirty or SharedCleanBytes (which ARE longer in length from Size)
                            offset = _sharedCleanBytes.Length - term.Length;
                        }
                        else if (_smapsBuffer[_currentBuffer][i] == 'P')
                        {
                            term = _privateCleanBytes; // or PrivateDirty (which is not longer in length from PrivateCleanBytes)
                        }
                        else if (_smapsBuffer[_currentBuffer][i] == 'L')
                        {
                            term = _lockedBytes;
                        }
                        else
                            continue;

                        // check if the current buffer too small for the search term and read next buff if so
                        if (switchBuffer == false && i + term.Length + offset > _endOfBuffer[_currentBuffer])
                        {
                            var nextBuffer = (_currentBuffer + 1) % 2;
                            read = ReadFromFile(fileStream, nextBuffer);
                            switchBuffer = true;
                        }

                        var searchedBuffer = _currentBuffer;
                        var positionToSearch = i;
                        var hasMatch = true;
                        for (var j = 1; j < term.Length; j++)
                        {
                            positionToSearch++;
                            if (positionToSearch == _smapsBuffer[searchedBuffer].Length)
                            {
                                // we assume max search term length doesn't exceed buffer length.. 
                                searchedBuffer = (searchedBuffer + 1) % 2;
                                positionToSearch = 0;
                                Debug.Assert(switchBuffer);
                            }

                            // for 'S' and 'P' we might have to search different term:
                            if (_smapsBuffer[searchedBuffer][positionToSearch] != term[j])
                            {
                                if (term == _privateCleanBytes) // didn't find PrivateCleanBytes - try to find PrivateDiryBytes
                                {
                                    term = _privateDirtyBytes;
                                    if (_smapsBuffer[searchedBuffer][positionToSearch] == term[j])
                                        continue;
                                }

                                if (term == _sizeBytes) // didn't find Size - try to find SharedCleanBytes
                                {
                                    term = _sharedCleanBytes;
                                    if (_smapsBuffer[searchedBuffer][positionToSearch] == term[j])
                                        continue;
                                }

                                if (term == _sharedCleanBytes) // didn't find SharedCleanBytes - try to find SharedDirtyBytes
                                {
                                    term = _sharedDirtyBytes;
                                    if (_smapsBuffer[searchedBuffer][positionToSearch] == term[j])
                                        continue;
                                }

                                hasMatch = false;
                                break;
                            }
                        }

                        if (hasMatch == false)
                            continue;

                        // now read value.. search until reached letter 'B' (value ends with "kB")
                        var bytesSearched = 0;
                        var posInTempBuf = 0;
                        var valueSearchPosition = i + term.Length;
                        var foundValue = false;
                        var foundK = false;

                        while (bytesSearched < _tempBufferBytes.Length) // just a bullpark figure, usually ~40 bytes are enough
                        {
                            if (valueSearchPosition == _smapsBuffer[searchedBuffer].Length)
                            {
                                searchedBuffer = (_currentBuffer + 1) % 2;
                                valueSearchPosition = 0;

                                if (switchBuffer == false)
                                {

                                    var readFromNextBuffer = ReadFromFile(fileStream, searchedBuffer);
                                    if (readFromNextBuffer == 0)
                                    {
                                        // this should not happen, the file ended without a value
                                        break;
                                    }

                                    switchBuffer = true;
                                }
                            }

                            var currentChar = _smapsBuffer[searchedBuffer][valueSearchPosition];

                            if (term == _rwsBytes) // value is filename which comes after last white-space and before '\n'
                            {
                                // zero previous entries
                                if (posInTempBuf == 0)
                                {
                                    resultString = null; // zero previous entries
                                    valSize = 0;
                                    valRss = 0;
                                    valPrivateDirty = 0;
                                    valSharedDirty = 0;
                                    valSharedClean = 0;
                                    valPrivateClean = 0;
                                }

                                if (currentChar == ' ' || currentChar == '\t')
                                    posInTempBuf = 0;
                                else if (currentChar == '\n')
                                    break;
                                else
                                {
                                    _tempBufferBytes[posInTempBuf++] = currentChar;
                                }
                            }
                            else
                            {
                                if (currentChar >= '0' && currentChar <= '9')
                                    _tempBufferBytes[posInTempBuf++] = currentChar;

                                if (currentChar == 'k')
                                    foundK = true;

                                if (currentChar == 'B')
                                {
                                    foundValue = true;
                                    break;
                                }
                            }

                            ++valueSearchPosition;
                            ++bytesSearched;
                        }

                        if (term != _rwsBytes)
                        {
                            if (foundValue == false)
                                ThrowNotContainsValidValue(term, currentProcess.Id);
                            if (foundK == false)
                                ThrowNotContainsKbValue(term, currentProcess.Id);
                        }
                        

                        i += term.Length + bytesSearched;
                        if (i >= _smapsBuffer[_currentBuffer].Length)
                            offsetForNextBuffer = _smapsBuffer[_currentBuffer].Length - i;
                        else
                            offsetForNextBuffer = 0;


                        long resultLong = 0;
                        if (term != _rwsBytes)
                        {
                            var multiplier = 1;
                            for (var j = posInTempBuf - 1; j >= 0; j--)
                            {
                                resultLong += (_tempBufferBytes[j] - (byte)'0') * multiplier;
                                multiplier *= 10;
                            }

                            resultLong *= 1024; // "kB"
                        }
                        else
                        {
                            resultString = posInTempBuf > 0 ? Encoding.UTF8.GetString(_tempBufferBytes, 0, posInTempBuf) : "";
                        }

                        if (term == _rwsBytes)
                        {
                            if (state != SearchState.None)
                                ThrowNotRwsTermAfterLockedTerm(state, term, currentProcess.Id);
                            state = SearchState.Rws;
                        }
                        else if (term == _sizeBytes)
                        {
                            if (state != SearchState.Rws)
                                continue; // found Rss but not after rw-s - irrelevant
                            state = SearchState.Size;
                            valSize = resultLong;
                        }
                        else if (term == _rssBytes)
                        {
                            if (state != SearchState.Size)
                                continue; // found Rss but not after rw-s - irrelevant
                            state = SearchState.Rss;
                            tmpRss += resultLong;
                            valRss = resultLong;
                        }
                        else if (term == _sharedCleanBytes)
                        {
                            if (state != SearchState.Rss)
                                continue; // found Shared_Clean but not after Rss (which must come after rw-s) - irrelevant
                            state = SearchState.SharedClean;
                            tmpSharedClean += resultLong;
                            valSharedClean = resultLong;
                        }
                        else if (term == _sharedDirtyBytes)
                        {
                            if (state != SearchState.SharedClean)
                                continue;
                            state = SearchState.SharedDirty;
                            valSharedDirty = resultLong;
                        }
                        else if (term == _privateCleanBytes)
                        {
                            if (state != SearchState.SharedDirty)
                                continue;
                            state = SearchState.PrivateClean;
                            tmpPrivateClean += resultLong;
                            valPrivateClean = resultLong;
                        }
                        else if (term == _privateDirtyBytes)
                        {
                            if (state != SearchState.PrivateClean)
                                continue;
                            state = SearchState.PrivateDirty;
                            valPrivateDirty = resultLong;
                        }
                        else if (term == _lockedBytes)
                        {
                            if (state != SearchState.PrivateDirty)
                                continue;
                            state = SearchState.None;

                            if (resultString == null)
                                ThrowOnNullString();

                            if (resultString.EndsWith(".voron") == false &&
                                resultString.EndsWith(".buffers") == false)
                                continue;

                            _smapsReaderResults.ResultString = resultString;
                            _smapsReaderResults.Size = valSize;
                            _smapsReaderResults.Rss = valRss;
                            _smapsReaderResults.SharedClean = valSharedClean;
                            _smapsReaderResults.SharedDirty = valSharedDirty;
                            _smapsReaderResults.PrivateClean = valPrivateClean;
                            _smapsReaderResults.PrivateDirty = valPrivateDirty;

                            smapResultsObject.Add(_smapsReaderResults);
                        }
                        else
                        {
                            throw new InvalidOperationException($"Reached unknown unhandled term: '{Encoding.UTF8.GetString(term)}'");
                        }
                    }


                    _currentBuffer = (_currentBuffer + 1) % 2;
                    if (switchBuffer == false)
                    {
                        read = ReadFromFile(fileStream, _currentBuffer);
                        if (read == 0)
                            break;
                    }
                }

                return (tmpRss, tmpSharedClean, tmpPrivateClean, smapResultsObject);
            }
        }

        private static void ThrowOnNullString()
        {
            throw new InvalidDataException("Got term 'Locked' (end of single mapping data) with no filename (in 'resultString') after rw-s");
        }

        private void ThrowNotRwsTermAfterLockedTerm(SearchState state, byte[] term, int processId)
        {
            throw new InvalidDataException(
                $"Found '{Encoding.UTF8.GetString(term)}' string in /proc/{processId}/smaps, but previous search did not end with '{Encoding.UTF8.GetString(_lockedBytes)}' (instead got {state})");
        }

        private void ThrowNotContainsValidValue(byte[] term, int processId)
        {
            throw new InvalidDataException($"Found '{Encoding.UTF8.GetString(term)}' string in /proc/{processId}/smaps, but no value");
        }

        private void ThrowNotContainsKbValue(byte[] term, int processId)
        {
            throw new InvalidDataException(
                $"Found '{Encoding.UTF8.GetString(term)}' string in /proc/{processId}/smaps, and value but not in kB - invalid format");
        }
    }
}

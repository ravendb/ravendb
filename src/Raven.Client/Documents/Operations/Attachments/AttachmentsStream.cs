using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments
{
    internal class AttachmentsStreamInfo
    {
        public AttachmentsStreamInfo()
        {
            AllStreams = new Dictionary<string, MemoryStream>();
            CurrentPosition = 0;
            CurrentIndex = 0;
        }

        public AttachmentsDetails AttachmentAdvancedDetails;
        public readonly Dictionary<string, MemoryStream> AllStreams;
        public JsonOperationContext.ManagedPinnedBuffer Buffer;

        public int CurrentIndex;
        public long CurrentPosition;
        public bool BufferConsumed;
    }

    internal class AttachmentsStream : Stream
    {
        private Stream _baseStream;
        private readonly string _name;
        private readonly AttachmentsStreamInfo _info;
        private readonly int _index;
        private readonly int _size;
        private bool _merged;

        public AttachmentsStream(Stream baseStream, string name, int index, int size, AttachmentsStreamInfo info)
        {
            if (baseStream == null)
                throw new ArgumentNullException(nameof(baseStream));
            if (baseStream.CanRead == false)
                throw new ArgumentException("can't read base stream");

            _baseStream = baseStream;
            _name = name;
            _info = info;
            _index = index;
            _size = size;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            CheckDisposed();

            if (buffer == null)
                throw new ArgumentNullException($"{nameof(buffer)} is null.");

            if (offset < 0 || count < 0)
                throw new ArgumentOutOfRangeException($"{nameof(offset)} or {nameof(count)} is negative.");

            count = Math.Min(_size, count);

            if (offset + count > buffer.Length)
                throw new ArgumentException($"The sum of {nameof(offset)} and {nameof(count)} is larger than the {nameof(buffer)} length.");

            lock (_info)
            {
                if (_info.AllStreams.TryGetValue(_name, out var memoryStream))
                {
                    if (memoryStream.Position == memoryStream.Length)
                        return 0;

                    var read = memoryStream.Read(buffer, offset, count);

                    Debug.Assert(count == read);
                    _info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read += read;
#if DEBUG
                    if (_info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read == _size)
                        Debug.Assert(_index < _info.CurrentIndex);
#endif

                    return read;
                }

                var startPosition = GetStartPosition();

                if (_info.BufferConsumed == false)
                {
                    return ReadFromBuffer(startPosition, buffer, offset, count);
                }

                if (startPosition == _info.CurrentPosition)
                {
                    if (_info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read == _info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Size)
                    {
                        // consumed stream
                        return 0;
                    }

                    var read = _baseStream.Read(buffer, offset, count);
                    if (read == 0)
                        throw new EndOfStreamException($"You have reached the end of the stream. {GetDebugInfo(count)}");

                    _info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read += read;
                    if (_info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read == _size)
                    {
                        Debug.Assert(_info.CurrentIndex == _index);
                        _info.CurrentIndex++;
                    }

                    _info.CurrentPosition += read;

                    if (_merged)
                    {
                        // we read both buffer and baseStream
                        read += _info.Buffer.Valid - _info.Buffer.Used;
                        _info.Buffer.Used += _info.Buffer.Valid - _info.Buffer.Used;
                        Debug.Assert(_info.Buffer.Valid == _info.Buffer.Used);
                    }

                    return read;
                }

                if (startPosition > _info.CurrentPosition)
                {
                    if (_info.AllStreams.TryGetValue(_info.AttachmentAdvancedDetails.AttachmentsMetadata[_info.CurrentIndex].Name, out var tmpStream))
                    {
                        var bSize = (int)_info.AttachmentAdvancedDetails.AttachmentsMetadata[_info.CurrentIndex].Size - _info.AttachmentAdvancedDetails.AttachmentsMetadata[_info.CurrentIndex].Read;
                        var toRead = bSize - (int)tmpStream.Length;
                        byte[] tmpBuffer = new byte[bSize];

                        // read from temp stream
                        var read = tmpStream.Read(tmpBuffer, 0, (int)tmpStream.Length);
                        Debug.Assert((int)tmpStream.Length == read);

                        // read from network stream
                        read = ReadInternal(tmpBuffer, (int)tmpStream.Length, toRead);
                        Debug.Assert(toRead == read);

                        CreateMemoryStreamFromBuffer(tmpBuffer, 0, bSize, _info.CurrentIndex, toRead);
                        _info.CurrentIndex++;

                        // dispose
                        tmpStream.Dispose();
                        _info.Buffer.Dispose();
                    }

                    for (int i = _info.CurrentIndex; i < _index; i++)
                    {
                        int localCount = (int)_info.AttachmentAdvancedDetails.AttachmentsMetadata[i].Size - _info.AttachmentAdvancedDetails.AttachmentsMetadata[_info.CurrentIndex].Read;
                        byte[] tmpBuffer = new byte[localCount];

                        ReadInternal(tmpBuffer, 0, localCount);
                        CreateMemoryStreamFromBuffer(tmpBuffer, 0, localCount, i);
                    }
#if DEBUG
                    if (startPosition != _info.CurrentPosition)
                    {
                        throw new Exception($"Attachment start on startPosition: {startPosition}, but stream CurrentPosition is {_info.CurrentPosition}. {GetDebugInfo(count)}");
                    }
#endif
                    _info.CurrentIndex = _index;

                    var n = _baseStream.Read(buffer, offset, count);
                    if (n == 0)
                        throw new EndOfStreamException($"You have reached the end of the base stream. {GetDebugInfo(count)}");

                    _info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read += n;
                    if (_info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read == _size)
                        _info.CurrentIndex++;

                    _info.CurrentPosition += n;
                    return n;
                }

                ThrowOnInvalidPosition(startPosition, count);
                return 0;
            }
        }

        private int ReadFromBuffer(long startPosition, byte[] buffer, int offset, int count)
        {
            if (startPosition == _info.CurrentPosition)
            {
                Debug.Assert(_info.CurrentIndex == _index);

                if (count + _info.Buffer.Used > _info.Buffer.Valid)
                {
                    var bufferRead = _info.Buffer.Valid - _info.Buffer.Used;
                    Array.Copy(_info.Buffer.Buffer.Array, _info.Buffer.Buffer.Offset + _info.Buffer.Used, buffer, offset, bufferRead);

                    _info.CurrentPosition += bufferRead;
                    _info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read += bufferRead;

                    // continue write to buffer from the base stream
                    _merged = true;
                    _info.BufferConsumed = true;
                    return Read(buffer, offset + bufferRead, count - bufferRead);
                }

                // read only from buffer
                Array.Copy(_info.Buffer.Buffer.Array, _info.Buffer.Buffer.Offset + _info.Buffer.Used, buffer, offset, count);

                _info.CurrentPosition += count;
                _info.Buffer.Used += count;
                _info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read += count;

                if (_info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read == _size)
                    _info.CurrentIndex = _index + 1;

                return count;
            }

            if (startPosition > _info.CurrentPosition)
            {
                int attachmentSize = (int)_info.AttachmentAdvancedDetails.AttachmentsMetadata[_info.CurrentIndex].Size - _info.AttachmentAdvancedDetails.AttachmentsMetadata[_info.CurrentIndex].Read;
                var bufferCount = _info.Buffer.Valid - _info.Buffer.Used;
                if (attachmentSize > bufferCount)
                {
                    // read to stream and add to dictionary
                    CreateMemoryStreamFromBuffer(_info.Buffer.Buffer.Array, _info.Buffer.Buffer.Offset + _info.Buffer.Used, bufferCount, _info.CurrentIndex);
                    _info.Buffer.Used += bufferCount;

                    // continue write to memory stream from the base stream
                    _info.BufferConsumed = true;
                    return Read(buffer, offset, count);
                }

                CreateMemoryStreamFromBuffer(_info.Buffer.Buffer.Array, _info.Buffer.Buffer.Offset + _info.Buffer.Used, attachmentSize, _info.CurrentIndex);

                _info.Buffer.Used += attachmentSize;
                _info.CurrentIndex++;

                return Read(buffer, offset, count);
            }

            ThrowOnInvalidPosition(startPosition, count);
            return 0;
        }

        private void CreateMemoryStreamFromBuffer(byte[] buffer, int offset, int count, int index, int? pos = null)
        {
            _info.AllStreams[_info.AttachmentAdvancedDetails.AttachmentsMetadata[index].Name] = new MemoryStream(buffer, offset, count, writable: false);
            _info.CurrentPosition += pos ?? count;
        }

        private long GetStartPosition()
        {
            var start = 0L;
            for (int i = 0; i < _index; i++)
            {
                start += _info.AttachmentAdvancedDetails.AttachmentsMetadata[i].Size;
            }

            return start + _info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Read;
        }

        private string GetDebugInfo(int count)
        {
            var s = $"\nCurrentPosition is {_info.CurrentPosition} / {_info.AttachmentAdvancedDetails.AttachmentsMetadata.Sum(x => x.Size)}, CurrentIndex: {_info.CurrentIndex}, bufferConsumed: {_info.BufferConsumed}, count: {count}. Requested attachment name: {_name}, startPosition: {GetStartPosition()} index: {_index}, merged: {_merged}.";
            s += "\nAttachments:";
            foreach (var attachment in _info.AttachmentAdvancedDetails.AttachmentsMetadata)
            {
                s += $"\n Name: {attachment.Name}, Index: {attachment.Index}, Read/Size: {attachment.Read} / {attachment.Size}";
            }

            s += "\nMemoryStreams:";
            foreach (var kvp in _info.AllStreams)
            {
                s += $"\n Name {kvp.Key}, length: {kvp.Value.Length}, position: {kvp.Value.Position}";
            }

            return s;
        }

        private int ReadInternal(byte[] buffer, int offset, int count)
        {
            int read = 0;

            do
            {
                var n = _baseStream.Read(buffer, offset, count);
                read += n;
                count -= n;
            } while (count > 0);

            return read;
        }

        private void CheckDisposed()
        {
            if (_baseStream == null)
                throw new ObjectDisposedException(GetType().Name);
        }

        public override long Length
        {
            get
            {
                CheckDisposed();
                return _info.AttachmentAdvancedDetails.AttachmentsMetadata[_index].Size;
            }
        }

        public override bool CanRead
        {
            get
            {
                CheckDisposed();
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                CheckDisposed();
                return false;
            }
        }

        public override bool CanSeek
        {
            get
            {
                CheckDisposed();
                return false;
            }
        }

        public override long Position
        {
            get
            {
                CheckDisposed();
                return _info.CurrentPosition;
            }
            set => throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Flush()
        {
            CheckDisposed();
            _baseStream.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing == false)
                return;

            // the caller is responsible for disposing the base stream
            _baseStream = null;

            foreach (var stream in _info.AllStreams)
                stream.Value?.Dispose();

            _info.Buffer?.Dispose();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("Writing to attachments stream is forbidden.");
        }

        private void ThrowOnInvalidPosition(long startPosition, int count)
        {
            if (startPosition < _info.CurrentPosition)
            {
                throw new ArgumentException($"The requested stream should have been in the dictionary already. {GetDebugInfo(count)}");
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow;

namespace Raven.Client.Documents.Operations.Attachments
{
    internal class AttachmentsStreamInfo
    {
        public AttachmentsStreamInfo()
        {
            CurrentPosition = 0;
            CurrentIndex = 0;
            BufferPosition = 0;
        }

        public List<AttachmentStreamDetails> AttachmentStreamDetails;

        public byte[] Buffer;
        public int BufferPosition;

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
        private readonly long _size;
        private bool _merged;

        public AttachmentsStream(Stream baseStream, string name, int index, long size, AttachmentsStreamInfo info)
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

            if (count > _size)
                count = (int)_size;

            if (offset + count > buffer.Length)
                throw new ArgumentException($"The sum of {nameof(offset)} and {nameof(count)} is larger than the {nameof(buffer)} length.");

            if (_info.AttachmentStreamDetails[_index].Read == _size)
            {
                Debug.Assert(_info.CurrentIndex > _index,
                    $"_info.AttachmentStreamDetails[_index].Read == _size && _info.CurrentIndex > _index.  {GetDebugInfo(count)}");
                return 0;
            }

            var startPosition = GetPositionFrom(from: 0);

            if (_info.BufferConsumed == false)
            {
                return ReadFromBuffer(startPosition, buffer, offset, count);
            }

            if (startPosition > _info.CurrentPosition)
            {
                Debug.Assert(_info.AttachmentStreamDetails[_index].Read == 0, "Cannot read part of attachment.");
                var toSkip = GetPositionFrom(_info.CurrentIndex);
                if (_info.AttachmentStreamDetails[_info.CurrentIndex].Read > 0)
                {
                    // merged attachment
                    toSkip -= _info.AttachmentStreamDetails[_info.CurrentIndex].Read;
                }

                _info.CurrentPosition = startPosition;
                _info.CurrentIndex = _index;

                while (toSkip > 0)
                {
                    int currentSkip = buffer.Length <= toSkip ? buffer.Length : (int)toSkip;
                    int read = _baseStream.Read(buffer, 0, currentSkip);
                    if (read == 0)
                        throw new EndOfStreamException($"You have reached the end of the stream while we tried to read {new Size(currentSkip, SizeUnit.Bytes)}. {GetDebugInfo(count)}");

                    toSkip -= read;
                }

                return Read(buffer, offset, count);
            }

            if (startPosition == _info.CurrentPosition)
            {
                if (_info.AttachmentStreamDetails[_index].Read == _info.AttachmentStreamDetails[_index].Size)
                {
                    // consumed stream
                    return 0;
                }

                var read = _baseStream.Read(buffer, offset, count);
                if (read == 0)
                    throw new EndOfStreamException($"You have reached the end of the stream. {GetDebugInfo(count)}");

                _info.AttachmentStreamDetails[_index].Read += read;
                if (_info.AttachmentStreamDetails[_index].Read == _size)
                {
                    Debug.Assert(_info.CurrentIndex == _index,
                        $"_info.AttachmentStreamDetails[_index].Read == _size && _info.CurrentIndex == _index. {GetDebugInfo(count)}");
                    _info.CurrentIndex++;
                }

                _info.CurrentPosition += read;

                if (_merged)
                {
                    // we read both buffer and baseStream
                    var readBuffer = _info.Buffer.Length - _info.BufferPosition;
                    read += readBuffer;
                    _info.BufferPosition += readBuffer;
                    Debug.Assert(_info.Buffer.Length == _info.BufferPosition,
                        $"startPosition == _info.CurrentPosition, _merged, _info.Buffer.Length == _info.BufferPosition. {GetDebugInfo(count)}");
                }

                return read;
            }

            Debug.Assert(startPosition < _info.CurrentPosition, $"The requested stream should have been read already. {GetDebugInfo(count)}");
            return 0;

        }

        private int ReadFromBuffer(long startPosition, byte[] buffer, int offset, int count)
        {
            if (startPosition == _info.CurrentPosition)
            {
                if (count + _info.BufferPosition > _info.Buffer.Length)
                {
                    var bufferRead = _info.Buffer.Length - _info.BufferPosition;
                    Array.Copy(_info.Buffer, _info.BufferPosition, buffer, offset, bufferRead);

                    _info.CurrentPosition += bufferRead;
                    _info.AttachmentStreamDetails[_index].Read += bufferRead;

                    // continue write to buffer from the base stream
                    _merged = true;
                    _info.BufferConsumed = true;
                    return Read(buffer, offset + bufferRead, count - bufferRead);
                }

                // read only from buffer
                Array.Copy(_info.Buffer, _info.BufferPosition, buffer, offset, count);

                _info.CurrentPosition += count;
                _info.BufferPosition += count;
                _info.AttachmentStreamDetails[_index].Read += count;

                if (_info.AttachmentStreamDetails[_index].Read == _size)
                    _info.CurrentIndex = _index + 1;

                return count;
            }

            if (startPosition > _info.CurrentPosition)
            {
                Debug.Assert(_info.AttachmentStreamDetails[_index].Read == 0, "Cannot read part of attachment.");
                if (startPosition >= _info.Buffer.Length)
                {
                    var mergedStartPosition = GetMergedPositionAndIndex(out var mergedIndex);
                    var bufferRead = _info.Buffer.Length - _info.BufferPosition;
                    _info.CurrentPosition += bufferRead;
                    _info.CurrentIndex = mergedIndex;

                    _info.AttachmentStreamDetails[mergedIndex].Read = _info.Buffer.Length - mergedStartPosition;
                    _info.BufferConsumed = true;
                }
                else
                {
                    _info.BufferPosition = (int)startPosition;
                    _info.CurrentPosition = startPosition;
                    _info.CurrentIndex = _index;
                }

                return Read(buffer, offset, count);
            }

            return 0;
        }

        private long GetPositionFrom(int from = 0)
        {
            var start = 0L;
            for (int i = from; i < _index; i++)
            {
                start += _info.AttachmentStreamDetails[i].Size;
            }

            return start + _info.AttachmentStreamDetails[_index].Read;
        }

        private int GetMergedPositionAndIndex(out int mergedIndex)
        {
            var start = _info.BufferPosition;
            var from = _info.CurrentIndex;
            while (start + _info.AttachmentStreamDetails[from].Size <= _info.Buffer.Length)
            {
                // _info.Buffer.Length is int, safe to cast to int here
                start += (int)_info.AttachmentStreamDetails[from].Size;
                from++;
            }

            // from is merged
            mergedIndex = from;
            return start;
        }

        private string GetDebugInfo(int count)
        {
            var s = $"\nCurrentPosition is {_info.CurrentPosition} / {_info.AttachmentStreamDetails.Sum(x => x.Size)}, CurrentIndex: {_info.CurrentIndex}, bufferConsumed: {_info.BufferConsumed}, count: {count}. Requested attachment name: {_name}, startPosition: {GetPositionFrom()} index: {_index}, merged: {_merged}.";
            s += "\nAttachments:";
            for (var index = 0; index < _info.AttachmentStreamDetails.Count; index++)
            {
                var attachment = _info.AttachmentStreamDetails[index];
                s += $"\n Index: {index}, Read/Size: {new Size(attachment.Read, SizeUnit.Bytes)} / {new Size(attachment.Size, SizeUnit.Bytes)}";
            }

            return s;
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
                return _info.AttachmentStreamDetails[_index].Size;
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
            _info.Buffer = null;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("Writing to attachments stream is forbidden.");
        }
    }
}

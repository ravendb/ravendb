using System;
using System.IO;
using System.Text;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public class BlittableObjectReader : IDisposable
    {
        private readonly StreamReader _reader;
        private readonly MemoryStream _ms;

        private StringBuilder _sb;
        private char[] _readBuffer;

        public int Capacity
        {
            get
            {
                if (_sb == null)
                    return _ms.Capacity;

                return _ms.Capacity + _sb.Capacity;
            }
        }

        public BlittableObjectReader()
        {
            _ms = new MemoryStream();
            _reader = new StreamReader(_ms, Encodings.Utf8, true, 1024, leaveOpen: true);
        }

        public TextReader GetTextReaderFor(BlittableJsonReaderObject value)
        {
            _reader.DiscardBufferedData();

            var ms = _reader.BaseStream;

            ms.Position = 0;
            value.WriteJsonTo(ms);
            ms.SetLength(ms.Position);
            ms.Position = 0;

            return _reader;
        }

        public string GetStringFor(BlittableJsonReaderObject value)
        {
            if (value == null)
                return null;

            GetTextReaderFor(value);

            if (_readBuffer == null)
                _readBuffer = new char[128];

            if (_sb == null)
                _sb = new StringBuilder();
            else
                _sb.Clear();

            int read;

            do
            {
                read = _reader.ReadBlock(_readBuffer, 0, _readBuffer.Length);
                _sb.Append(_readBuffer, 0, read);

            } while (read == _readBuffer.Length);

            return _sb.ToString();
        }

        public void ResetCapacity()
        {
            _reader.DiscardBufferedData();

            if (_sb != null)
            {
                _sb.Clear();
                _sb.Capacity = 16;
            }

            _ms.SetLength(0);
            _ms.Capacity = 64;
        }

        public void Dispose()
        {
            _reader?.Dispose();
            _ms?.Dispose();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Utilities;

namespace Raven.Abstractions.Json
{
	public class RavenJsonTextReader : JsonTextReader
	{
		public RavenJsonTextReader(TextReader reader)
			: base(reader)
		{
			DateParseHandling = DateParseHandling.None;
		}

	    public RavenJsonTextReader(char[] externalBuffer) : base(externalBuffer)
	    {
            DateParseHandling = DateParseHandling.None;
        }

	    private static TimeSpan ReadOffset(string offsetText)
		{
			bool negative = (offsetText[0] == '-');

			int hours = int.Parse(offsetText.Substring(1, 2), NumberStyles.Integer, CultureInfo.InvariantCulture);
			int minutes = 0;
			if (offsetText.Length >= 5)
				minutes = int.Parse(offsetText.Substring(3, 2), NumberStyles.Integer, CultureInfo.InvariantCulture);

			TimeSpan offset = TimeSpan.FromHours(hours) + TimeSpan.FromMinutes(minutes);
			if (negative)
				offset = offset.Negate();

			return offset;
		}

		public static DateTime ParseDateMicrosoft(string text)
		{
			string value = text.Substring(6, text.Length - 8);

			int index = value.IndexOf('+', 1);

			if (index == -1)
				index = value.IndexOf('-', 1);

			if (index != -1)
			{
				value = value.Substring(0, index);
			}

			long javaScriptTicks = long.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);

			DateTime utcDateTime = DateTimeUtils.ConvertJavaScriptTicksToDateTime(javaScriptTicks);
			return utcDateTime;
		}
	}

    public class RavenJsonTextReaderFromStream : RavenJsonTextReader
    {
        private readonly Stream stream;
        private Buffer bufferObj;

        private class Buffer
        {
            public byte[] Bytes;
            public char[] Chars;
        }

        [ThreadStatic] private static LinkedList<Buffer> buffers;
        private readonly Decoder decoder;

        private int bufferPos;
        private int bufferUsedLen;
        private static readonly byte[] Utf8Preamble = Encoding.UTF8.GetPreamble();
        private bool checkedPreamble;

        public static RavenJsonTextReader Create(Stream stream)
        {
            if (buffers == null)
                buffers = new LinkedList<Buffer>();
            Buffer buffer;
            if (buffers.Count != 0)
            {
                buffer = buffers.First.Value;
                buffers.RemoveFirst();
            }
            else
            {
                buffer = new Buffer
                {
                    Bytes = new byte[1024],
                    Chars = new char[Encoding.UTF8.GetMaxCharCount(1024)]
                };
            }
            buffer.Chars[0] = '\0';
            return new RavenJsonTextReaderFromStream(stream, buffer);
        }


        private RavenJsonTextReaderFromStream(Stream stream, Buffer bufferObj) : base(bufferObj.Chars)
        {
            this.stream = stream;
            this.bufferObj = bufferObj;
            decoder = Encoding.UTF8.GetDecoder();
        }

        protected override int ReadChars(char[] buffer, int start, int count)
        {
            if (bufferPos < bufferUsedLen)
            {
                if (checkedPreamble == false)
                {
                    checkedPreamble = true;
                    while (bufferUsedLen < Utf8Preamble.Length)
                    {
                        var read = stream.Read(bufferObj.Bytes, bufferUsedLen, bufferObj.Bytes.Length - bufferUsedLen);
                        if (read == 0)
                            break;
                        bufferUsedLen += read;
                    }
                    if (bufferUsedLen >= Utf8Preamble.Length)
                    {
                        bool hasPreamble = true;
                        for (int i = 0; i < Utf8Preamble.Length; i++)
                        {
                            if (bufferObj.Bytes[i] != Utf8Preamble[i])
                            {
                                hasPreamble = false;
                                break;
                            }
                        }
                        if (hasPreamble)
                        {
                            bufferPos += Utf8Preamble.Length;
                            if (bufferPos < bufferUsedLen)
                                return ReadChars(buffer, start, count);
                        }
                    }
                }

                int bytesUsed;
                int charsUsed;
                bool completed;
                decoder.Convert(bufferObj.Bytes, bufferPos, bufferUsedLen - bufferPos, buffer, start, count, false, 
                    out bytesUsed, out charsUsed, out completed);
                bufferPos += bytesUsed;
                return charsUsed;
            }
            bufferUsedLen = stream.Read(bufferObj.Bytes,0, bufferObj.Bytes.Length);
            bufferPos = 0;
            if (bufferUsedLen == 0)
                return 0;

            return ReadChars(buffer, start, count);
        }

        public override void Close()
        {
            if (buffers == null)
                buffers = new LinkedList<Buffer>();
            if (bufferObj != null)
                buffers.AddFirst(bufferObj);
            bufferObj = null;
           
            base.Close();
        }
    }
}
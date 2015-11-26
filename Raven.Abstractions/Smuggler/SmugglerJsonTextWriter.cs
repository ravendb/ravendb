using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Exceptions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Utilities;

namespace Raven.Abstractions.Smuggler
{
    // JsonTextWriter wrapper to enable split files
    // Assumes caller keeps valid json structure (matching Start/End Object/Array writes)
    public class SmugglerJsonTextWriter : IDisposable
    {
#if FALSE
        private const long Mb = 1024L * 1024;
#else
        private const long Mb = 1;
#endif
        private readonly int splitSizeMb;

        private readonly string filepath;
        private int splitsCount;
        private readonly LinkedList<JsonToken> jsonOperationsStructure;
        private string lastPropertyName;
        public Formatting Formatting { get; set; }

        private CountingStream currentCountingStream;
        private JsonTextWriter currentJsonTextWriter;
#if TRUE
        private GZipStream currentGzipStream;
#else
        private CountingStream currentGzipStream;
#endif
        private StreamWriter currentStreamWriter;
        private Stream currentStream;

        public JsonTextWriter GetCurrentJsonTextWriter()
        {
            return currentJsonTextWriter;
        }

        public SmugglerJsonTextWriter(StreamWriter streamWriter, int splitSizeMb, CountingStream countingStream, string filepath)
        {
            this.currentStream = streamWriter.BaseStream;
            this.currentStreamWriter = streamWriter;
            this.currentCountingStream = countingStream;
            this.splitSizeMb = splitSizeMb;
            this.filepath = filepath;
            this.currentJsonTextWriter = new JsonTextWriter(streamWriter)
            {
                Formatting = this.Formatting
            };
            jsonOperationsStructure = new LinkedList<JsonToken>();
        }

        public SmugglerJsonTextWriter(StringWriter stringWriter)
        {
            this.splitSizeMb = 0;
            this.currentJsonTextWriter = new JsonTextWriter(stringWriter);
            jsonOperationsStructure = new LinkedList<JsonToken>();
        }

        private void SpinWriterIfReachedMaxSize()
        {
            if (splitSizeMb == 0 || currentCountingStream.NumberOfWrittenBytes < splitSizeMb * Mb)
                return;

            if (jsonOperationsStructure.Count != 2)
                return; // don't break the file in the middle of a document

            while (jsonOperationsStructure.Count > 0)
            {
                var op = jsonOperationsStructure.Last.Value;
                if (op == JsonToken.StartArray)
                    WriteEndArray();
                else if (op == JsonToken.StartObject)
                    WriteEndObject();
                // ADIADI :: TODO: We can't just close a constructor in the middle and reopen it in the next file. 'return' here could be nice to let the current file continues untill 'WriteEnd(JsonToken.EndConstructor)' is called, but this method is protected so we need to find a way to know if the constructor is closed. See "WriteStartConstructor" 
                // else if (op == JsonToken.StartConstructor)
                //     return;
            }


            DisposeStreams();

            currentStream = File.Create($"{filepath}.part{++splitsCount:D3}");

            currentCountingStream = new CountingStream(currentStream);
#if TRUE
            currentGzipStream = new GZipStream(currentCountingStream, CompressionMode.Compress, leaveOpen: true);
            currentStreamWriter = new StreamWriter(currentGzipStream);
#else
            currentStreamWriter = new StreamWriter(currentCountingStream);
#endif

            currentJsonTextWriter = new JsonTextWriter(currentStreamWriter)
            {
                Formatting = this.Formatting
            };

            WriteStartObject();
            WritePropertyName(lastPropertyName);
            WriteStartArray();
        }

        private void DisposeStreams()
        {
            currentStreamWriter?.Flush();

            // the first stream is the original instance, the caller is responsible to dispose it
            // the newer instances are created here and should be disposed
            if (splitsCount > 0)
            {
                currentStreamWriter?.Dispose();
                currentStreamWriter = null;
                currentGzipStream?.Dispose();
                currentGzipStream = null;
                currentCountingStream?.Dispose();
                currentCountingStream = null;
                currentStream?.Dispose();
                currentStream = null;
            }
        }

        public void Flush()
        {
            currentJsonTextWriter.Flush();
        }

        public void WriteStartObject()
        {
            jsonOperationsStructure.AddLast(JsonToken.StartObject);
            GetCurrentJsonTextWriter().WriteStartObject();
        }

        public void WritePropertyName(string name)
        {
            GetCurrentJsonTextWriter().WritePropertyName(name);

            if (jsonOperationsStructure.Count == 1)
                lastPropertyName = name;
        }

        public void WritePropertyName(string name, bool escape)
        {
            GetCurrentJsonTextWriter().WritePropertyName(name, escape);
            if (jsonOperationsStructure.Count == 1)
                lastPropertyName = name;
        }

        public void WriteStartArray()
        {
            jsonOperationsStructure.AddLast(JsonToken.StartArray);
            GetCurrentJsonTextWriter().WriteStartArray();
        }

        public void WriteEndArray()
        {
            jsonOperationsStructure.RemoveLast();
            GetCurrentJsonTextWriter().WriteEndArray();
        }

        public void WriteEndObject()
        {
            jsonOperationsStructure.RemoveLast();
            GetCurrentJsonTextWriter().WriteEndObject();

            // After WriteEndObject we might get an end of a document which is a good spot to split if limit reached
            SpinWriterIfReachedMaxSize();
        }

        public void WriteStartConstructor(string name)
        {
            // TODO: WriteEnd(JsonToken.EndConstructor) is protected non-virtual so we need to find a way to know if stream is about to be splitted in the middle of the 'Constructor'
            // jsonOperationsStructure.AddLast(JsonToken.StartConstructor);
            GetCurrentJsonTextWriter().WriteStartConstructor(name);
        }

        // TODO: WriteEnd
        //protected void WriteEnd(JsonToken token)
        //{
        //    switch (token)
        //    {
        //        case JsonToken.EndObject:
        //        case JsonToken.EndArray:
        //        case JsonToken.EndConstructor:
        //            jsonOperationsStructure.RemoveLast();
        //            break;
        //        default:
        //            throw JsonWriterException.Create(this, "Invalid JsonToken: " + token, null);
        //    }

        //    GetCurrentJsonTextWriterWithNoSpin().WriteEnd(token);
        //}




        public void WriteValue(byte[] value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteNull()
        {
            GetCurrentJsonTextWriter().WriteNull();
        }

        public void WriteUndefined()
        {
            GetCurrentJsonTextWriter().WriteUndefined();
        }

        public void WriteRaw(string json)
        {
            GetCurrentJsonTextWriter().WriteRaw(json);
        }

        public void WriteRawValue(string json)
        {
            GetCurrentJsonTextWriter().WriteRawValue(json);
        }

        public void WriteValue(object value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(string value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(int value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(uint value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(long value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(ulong value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(float value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(float? value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(double value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(double? value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(bool value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(short value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(ushort value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(char value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(byte value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(sbyte value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(decimal value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(DateTime value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(DateTimeOffset value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(Guid value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(TimeSpan value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteValue(Uri value)
        {
            GetCurrentJsonTextWriter().WriteValue(value);
        }

        public void WriteComment(string text)
        {
            GetCurrentJsonTextWriter().WriteComment(text);
        }

        public void WriteWhitespace(string ws)
        {
            GetCurrentJsonTextWriter().WriteWhitespace(ws);
        }

        public void Dispose()
        {
            DisposeStreams();
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

using Raven.Abstractions.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerJsonTextWriter : IDisposable
    {
        private const long Mb = 1024L * 1024;
        private readonly int splitSizeMb;

        private readonly Formatting formatting;

        private readonly string filepath;
        private int splitsCount;
        private readonly LinkedList<JsonToken> jsonOperationsStructure;
        private string lastPropertyName;

        private CountingStream currentCountingStream;
        private JsonTextWriter currentJsonTextWriter;
        private GZipStream currentGzipStream;
        private StreamWriter currentStreamWriter;
        private Stream currentStream;

        public JsonTextWriter GetCurrentJsonTextWriter()
        {
            return currentJsonTextWriter;
        }

        public SmugglerJsonTextWriter(StreamWriter streamWriter, int splitSizeMb, Formatting formatting, CountingStream countingStream, string filepath)
        {
            this.currentStream = streamWriter.BaseStream;
            this.currentStreamWriter = streamWriter;
            this.currentCountingStream = countingStream;
            this.splitSizeMb = splitSizeMb;
            this.formatting = formatting;
            this.filepath = filepath;
            this.currentJsonTextWriter = new JsonTextWriter(streamWriter)
            {
                Formatting = formatting
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
            }

            DisposeStreams();

            currentStream = File.Create($"{filepath}.part{++splitsCount:D3}");

            currentCountingStream = new CountingStream(currentStream);
            currentGzipStream = new GZipStream(currentCountingStream, CompressionMode.Compress, leaveOpen: true);
            currentStreamWriter = new StreamWriter(currentGzipStream);
            currentJsonTextWriter = new JsonTextWriter(currentStreamWriter)
            {
                Formatting = formatting
            };

            WriteStartObject();
            WritePropertyName(lastPropertyName);
            WriteStartArray();
        }

        public void Write(RavenJObject ravenJObject, params JsonConverter[] converters)
        {
            ravenJObject.WriteTo(GetCurrentJsonTextWriter(), converters);
            SpinWriterIfReachedMaxSize();
        }

        public void Write(RavenJArray ravenJArray, params JsonConverter[] converters)
        {
            ravenJArray.WriteTo(GetCurrentJsonTextWriter(), converters);
            SpinWriterIfReachedMaxSize();
        }

        public void Write(RavenJToken ravenJToken, params JsonConverter[] converters)
        {
            ravenJToken.WriteTo(GetCurrentJsonTextWriter(), converters);
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
        
        public void Dispose()
        {
            DisposeStreams();
        }

    }
}

using System;
using System.IO;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Streaming
{
    public class JsonOutputWriter : IOutputWriter
    {
        private const string JsonContentType = "application/json";
        private readonly Stream stream;
        private JsonWriter writer;
        private bool closedArray = false;

        public JsonOutputWriter(Stream stream)
        {
            this.stream = stream;
        }

        public string ContentType => JsonContentType;

        public void WriteHeader()
        {
            writer = new JsonTextWriter(new StreamWriter(stream));
            writer.WriteStartObject();
            writer.WritePropertyName("Results");
            writer.WriteStartArray();
        }

        public void Dispose()
        {
            if (writer == null)
                return;
            if (closedArray == false)
                writer.WriteEndArray();
            writer.WriteEndObject();
            writer.WriteRaw(Environment.NewLine);

            writer.Flush();
            stream.Flush();
            writer.Close();
        }

        public void Write(RavenJObject result)
        {
            result.WriteTo(writer, Default.Converters);
            writer.WriteRaw(Environment.NewLine);
        }

        public void WriteError(Exception exception)
        {
            closedArray = true;
            writer.WriteEndArray();
            writer.WritePropertyName("Error");
            writer.WriteValue(exception.ToString());
        }

        public void Flush()
        {
            writer.Flush();
        }
    }
}
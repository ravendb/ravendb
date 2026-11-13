using System;
using System.Collections.Generic;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public sealed class JsonPatchDocument
    {
        internal List<OperationEntry> Operations { get; } = new();

        public JsonPatchDocument Add(string path, object value)
        {
            return Append("add", path, from: null, value);
        }

        public JsonPatchDocument Remove(string path)
        {
            return Append("remove", path, from: null, value: null);
        }

        public JsonPatchDocument Replace(string path, object value)
        {
            return Append("replace", path, from: null, value);
        }

        public JsonPatchDocument Test(string path, object value)
        {
            return Append("test", path, from: null, value);
        }

        public JsonPatchDocument Move(string from, string path)
        {
            return Append("move", path, NormalizePath(from, nameof(from)), value: null);
        }

        public JsonPatchDocument Copy(string from, string path)
        {
            return Append("copy", path, NormalizePath(from, nameof(from)), value: null);
        }

        internal List<BlittableJsonReaderObject> ToBlittable(ISerializationConventions serialization, JsonOperationContext context)
        {
            IJsonSerializer serializer = serialization.CreateSerializer(new CreateSerializerOptions { TypeNameHandling = TypeNameHandling.None });
            var operations = new List<BlittableJsonReaderObject>(Operations.Count);

            foreach (OperationEntry operation in Operations)
            {
                using (IJsonWriter writer = serialization.CreateWriter(context))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("op");
                    writer.WriteValue(operation.Op);
                    writer.WritePropertyName("path");
                    writer.WriteValue(operation.Path);

                    if (operation.From != null)
                    {
                        writer.WritePropertyName("from");
                        writer.WriteValue(operation.From);
                    }

                    if (operation.HasValue)
                    {
                        writer.WritePropertyName("value");
                        serializer.Serialize(writer, operation.Value);
                    }

                    writer.WriteEndObject();
                    writer.FinalizeDocument();
                    operations.Add(writer.CreateReader());
                }
            }

            return operations;
        }

        private JsonPatchDocument Append(string op, string path, string from, object value)
        {
            Operations.Add(new OperationEntry(op, NormalizePath(path, nameof(path)), from, value));
            return this;
        }

        private static string NormalizePath(string path, string parameterName)
        {
            if (path == null)
                throw new ArgumentNullException(parameterName);

            if (path.Contains("//"))
                throw new ArgumentException($"The path '{path}' is invalid.", parameterName);

            return path.StartsWith("/", StringComparison.Ordinal) ? path : "/" + path;
        }

        internal sealed class OperationEntry
        {
            public OperationEntry(string op, string path, string from, object value)
            {
                Op = op;
                Path = path;
                From = from;
                Value = value;
            }

            public string Op { get; }

            public string Path { get; }

            public string From { get; }

            public object Value { get; }

            public bool HasValue => Op is "add" or "replace" or "test";
        }
    }
}

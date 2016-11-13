using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using  Raven.Imports.Newtonsoft.Json;
using  Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Json.Linq;
using Raven.NewClient.Json.Linq;

namespace Raven.Abstractions.Json
{
    public class ConflictsResolver
    {
        private readonly RavenJObject[] docs;
        private readonly bool isMetadataResolver;

        public ConflictsResolver(IEnumerable<RavenJObject> docs, bool isMetadataResolver = false)
        {
            this.docs = docs.ToArray();
            this.isMetadataResolver = isMetadataResolver;
        }

        public MergeResult Resolve(int indent = 1)
        {
            var result = new Dictionary<string, object>();
            for (var index = 0; index < docs.Length; index++)
            {
                var doc = docs[index];
                foreach (var prop in doc)
                {
                    if (result.ContainsKey(prop.Key)) // already dealt with
                        continue;

                    switch (prop.Value.Type)
                    {
                        case JTokenType.Object:
                            if (TryHandleObjectValue(index, result, prop) == false)
                                goto default;
                            break;
                        case JTokenType.Array:
                            if (TryHandleArrayValue(index, result, prop) == false)
                                goto default;
                            break;
                        default:
                            HandleSimpleValues(result, prop, index);
                            break;
                    }
                }
            }
            return GenerateOutput(result, indent);
        }

        private bool TryHandleArrayValue(int index, Dictionary<string, object> result, KeyValuePair<string, RavenJToken> prop)
        {
            var arrays = new List<RavenJArray>
            {
                (RavenJArray)prop.Value
            };

            for (var i = 0; i < docs.Length; i++)
            {
                if (i == index)
                    continue;

                RavenJToken token;
                if (docs[i].TryGetValue(prop.Key, out token) && token.Type != JTokenType.Array)
                    return false;
                if (token == null)
                    continue;
                if (token.IsSnapshot)
                    token = token.CreateSnapshot();
                arrays.Add((RavenJArray)token);
            }

            var mergedArray = new RavenJArray();
            while (arrays.Count > 0)
            {
                var set = new HashSet<RavenJToken>(RavenJTokenEqualityComparer.Default);
                for (var i = 0; i < arrays.Count; i++)
                {
                    if (arrays[i].Length == 0)
                    {
                        arrays.RemoveAt(i);
                        i -= 1;
                        continue;
                    }
                    set.Add(arrays[i][0]);
                    arrays[i].RemoveAt(0);
                }

                foreach (var ravenJToken in set)
                {
                    mergedArray.Add(ravenJToken);
                }
            }

            if (RavenJTokenEqualityComparer.Default.Equals(mergedArray, prop.Value))
            {
                result.Add(prop.Key, mergedArray);
                return true;
            }

            result.Add(prop.Key, new ArrayWithWarning(mergedArray));
            return true;
        }

        private bool TryHandleObjectValue(int index, Dictionary<string, object> result, KeyValuePair<string, RavenJToken> prop)
        {
            var others = new List<RavenJObject>
            {
                (RavenJObject)prop.Value
            };
            for (var i = 0; i < docs.Length; i++)
            {
                if (i == index)
                    continue;

                RavenJToken token;
                if (docs[i].TryGetValue(prop.Key, out token) && token.Type != JTokenType.Object)
                    return false;
                if (token == null)
                    continue;
                others.Add((RavenJObject)token);
            }

            result.Add(prop.Key, new ConflictsResolver(others.ToArray(), prop.Key == "@metadata" || isMetadataResolver));
            return true;
        }

        private void HandleSimpleValues(Dictionary<string, object> result, KeyValuePair<string, RavenJToken> prop, int index)
        {
            var conflicted = new Conflicted
            {
                Values = { prop.Value }
            };

            for (var i = 0; i < docs.Length; i++)
            {
                if (i == index)
                    continue;
                var other = docs[i];

                RavenJToken otherVal;
                if (other.TryGetValue(prop.Key, out otherVal) == false)
                    continue;
                if (RavenJTokenEqualityComparer.Default.Equals(prop.Value, otherVal) == false)
                    conflicted.Values.Add(otherVal);
            }

            if (conflicted.Values.Count == 1)
                result.Add(prop.Key, prop.Value);
            else
                result.Add(prop.Key, conflicted);
        }

        private void WriteToken(JsonTextWriter writer, string propertyName, Object propertyValue)
        {
            if (isMetadataResolver && (
                propertyName.StartsWith("Raven-Replication-") ||
                propertyName.StartsWith("@") ||
                propertyName == "Last-Modified" ||
                propertyName == "Raven-Last-Modified"
                ) )
            {
                return;
            }

            writer.WritePropertyName(propertyName);
            var ravenJToken = propertyValue as RavenJToken;
            if (ravenJToken != null)
            {
                ravenJToken.WriteTo(writer);
                return;
            }
            var conflicted = propertyValue as Conflicted;
            if (conflicted != null)
            {
                writer.WriteComment(">>>> conflict start");
                writer.WriteStartArray();
                foreach (var token in conflicted.Values)
                {
                    token.WriteTo(writer);
                }
                writer.WriteEndArray();
                writer.WriteComment("<<<< conflict end");
                return;
            }

            var arrayWithWarning = propertyValue as ArrayWithWarning;
            if (arrayWithWarning != null)
            {
                writer.WriteComment(">>>> auto merged array start");
                arrayWithWarning.MergedArray.WriteTo(writer);
                writer.WriteComment("<<<< auto merged array end");
                return;
            }

            throw new InvalidOperationException("Could not understand how to deal with: " + propertyValue);
        }

        private void WriteRawData(JsonTextWriter writer, String data, int indent)
        {
            var sb = new StringBuilder();
            using (var stringReader = new StringReader(data))
            {
                var first = true;
                string line;
                while ((line = stringReader.ReadLine()) != null)
                {
                    if (first == false)
                    {
                        sb.AppendLine();
                        for (var i = 0; i < indent; i++)
                        {
                            sb.Append(writer.IndentChar, writer.Indentation);
                        }
                    }

                    sb.Append(line);

                    first = false;
                }
            }
            writer.WriteRawValue(sb.ToString());
        }

        private void WriteConflictResolver(string name, JsonTextWriter documentWriter, JsonTextWriter metadataWriter, ConflictsResolver resolver, int indent)
        {
            MergeResult result = resolver.Resolve(indent);

            if (resolver.isMetadataResolver)
            {
                if(name != "@metadata")
                    metadataWriter.WritePropertyName(name);

                WriteRawData(metadataWriter, result.Document, indent);
            }
            else
            {
                documentWriter.WritePropertyName(name);
                WriteRawData(documentWriter, result.Document, indent);
            }
        }

        private MergeResult GenerateOutput(Dictionary<string, object> result, int indent)
        {
            var documentStringWriter = new StringWriter();
            var documentWriter = new JsonTextWriter(documentStringWriter)
            {
                Formatting = Formatting.Indented,
                IndentChar = '\t',
                Indentation = 1
            };

            var metadataStringWriter = new StringWriter();
            var metadataWriter = new JsonTextWriter(metadataStringWriter)
            {
                Formatting = Formatting.Indented,
                IndentChar = '\t',
                Indentation = 1
            };

            documentWriter.WriteStartObject();
            foreach (var o in result)
            {
                var resolver = o.Value as ConflictsResolver;
                if (resolver != null)
                {
                    WriteConflictResolver(o.Key, documentWriter, metadataWriter, resolver, o.Key == "@metadata" ? 0 : indent + 1);
                }
                else
                {
                    WriteToken(o.Key == "@metadata" ? metadataWriter : documentWriter, o.Key, o.Value);
                }
            }
            documentWriter.WriteEndObject();

            return new MergeResult()
            {
                Document = documentStringWriter.GetStringBuilder().ToString(),
                Metadata = metadataStringWriter.GetStringBuilder().ToString()
            };
        }

        private class Conflicted
        {
            public readonly HashSet<RavenJToken> Values = new HashSet<RavenJToken>(RavenJTokenEqualityComparer.Default);
        }

        private class ArrayWithWarning
        {
            public readonly RavenJArray MergedArray;

            public ArrayWithWarning(RavenJArray mergedArray)
            {
                MergedArray = mergedArray;
            }
        }

        public class MergeChunk
        {
            public bool IsMetadata { get; set; }
            public string Data { get; set; }
        }

        public class MergeResult
        {
            public string Document { get; set; }
            public string Metadata { get; set; }
        }
    }
}

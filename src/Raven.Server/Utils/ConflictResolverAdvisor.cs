using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Utils
{
    public class ConflictResolverAdvisor
    {
        private readonly BlittableJsonReaderObject[] _docs;
        internal readonly bool IsMetadataResolver;
        private readonly JsonOperationContext _context;

        public ConflictResolverAdvisor(IEnumerable<BlittableJsonReaderObject> docs, JsonOperationContext ctx, bool isMetadataResolver = false)
        {
            _docs = docs.ToArray();
            IsMetadataResolver = isMetadataResolver;
            _context = ctx;
        }

        public MergeResult Resolve(int indent = 1)
        {
            var result = new Dictionary<string, object>();
            for (var index = 0; index < _docs.Length; index++)
            {
                var doc = _docs[index];
                if (doc == null)
                    continue; // we never suggest to delete a document
                for (var indexProp = 0; indexProp < doc.Count; indexProp++)
                {
                    BlittableJsonReaderObject.PropertyDetails prop = new BlittableJsonReaderObject.PropertyDetails();
                    doc.GetPropertyByIndex(indexProp, ref prop);

                    if (result.ContainsKey(prop.Name)) // already dealt with
                        continue;

                    prop.Token = doc.ProcessTokenTypeFlags(prop.Token);
                    switch (prop.Token)
                    {
                        case BlittableJsonToken.StartObject:
                        case BlittableJsonToken.EmbeddedBlittable:
                            var objTuple = new KeyValuePair<string, BlittableJsonReaderObject>(prop.Name, (BlittableJsonReaderObject)prop.Value);
                            if (TryHandleObjectValue(index, result, objTuple) == false)
                                goto default;
                            break;
                        case BlittableJsonToken.StartArray:
                            var arrTuple = new KeyValuePair<string, BlittableJsonReaderArray>(prop.Name, (BlittableJsonReaderArray)prop.Value);
                            if (TryHandleArrayValue(index, result, arrTuple) == false)
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

        private bool TryHandleObjectValue(int index, Dictionary<string, object> result, KeyValuePair<string, BlittableJsonReaderObject> prop)
        {
            var others = new List<BlittableJsonReaderObject>
            {
                prop.Value
            };
            for (var i = 0; i < _docs.Length; i++)
            {
                if (i == index)
                    continue;

                if (_docs[i] == null)
                    continue;
                if (_docs[i].TryGetWithoutThrowingOnError(prop.Key, out BlittableJsonReaderObject token) == false)
                    return false;
                if (token == null)
                    continue;
                others.Add(token);
            }

            result.Add(prop.Key, new ConflictResolverAdvisor(
                others.ToArray(), _context, prop.Key == Constants.Documents.Metadata.Key || IsMetadataResolver));
            return true;
        }

        private bool TryHandleArrayValue(int index, Dictionary<string, object> result, KeyValuePair<string, BlittableJsonReaderArray> prop)
        {
            var arrays = new List<BlittableJsonReaderArray>
            {
                prop.Value
            };

            for (var i = 0; i < _docs.Length; i++)
            {
                if (i == index)
                    continue;

                if (_docs[i].TryGetWithoutThrowingOnError(prop.Key, out BlittableJsonReaderArray token) == false)
                    return false;
                if (token == null)
                    continue;
                arrays.Add(token);
            }

            var set = new HashSet<Tuple<object, BlittableJsonToken>>();
            var lastLength = arrays[0].Length;
            var sameSize = true;
            foreach (var arr in arrays)
            {
                sameSize = arr.Length == lastLength;
                for (var propIndex = 0; propIndex < arr.Length; propIndex++)
                {
                    var tuple = arr.GetValueTokenTupleByIndex(propIndex);
                    set.Add(tuple);
                }
            }
            BlittableJsonReaderArray reader;
            using (var mergedArray = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(_context))
            {
                mergedArray.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                mergedArray.StartArrayDocument();
                mergedArray.StartWriteArray();
                foreach (var item in set)
                {
                    mergedArray.WriteValue(item.Item2 & BlittableJsonReaderBase.TypesMask, item.Item1);
                }
                mergedArray.WriteArrayEnd();
                mergedArray.FinalizeDocument();
                reader = mergedArray.CreateArrayReader();
            }

            if (sameSize && prop.Value.Equals(reader))
            {
                result.Add(prop.Key, reader);
                return true;
            }

            result.Add(prop.Key, new ArrayWithWarning(reader));
            return true;
        }

        private void HandleSimpleValues(Dictionary<string, object> result, BlittableJsonReaderObject.PropertyDetails prop, int index)
        {
            var conflicted = new Conflicted
            {
                Values = { prop }
            };

            for (var i = 0; i < _docs.Length; i++)
            {
                if (i == index)
                    continue;
                var other = _docs[i];
                if (other == null)
                    continue;

                BlittableJsonReaderObject.PropertyDetails otherProp = new BlittableJsonReaderObject.PropertyDetails();
                var propIndex = other.GetPropertyIndex(prop.Name);
                if (propIndex == -1)
                {
                    continue;
                }
                other.GetPropertyByIndex(propIndex, ref otherProp);

                if (otherProp.Token != prop.Token ||// if type is null there could not be a conflict
                    (prop.Value?.Equals(otherProp.Value) == false)
                    )
                {
                    conflicted.Values.Add(otherProp);
                }
            }

            if (conflicted.Values.Count == 1)
            {
                result.Add(prop.Name, prop);
            }
            else
            {
                result.Add(prop.Name, conflicted);
            }
        }

        private class Conflicted
        {
            public readonly HashSet<BlittableJsonReaderObject.PropertyDetails> Values = new HashSet<BlittableJsonReaderObject.PropertyDetails>();
        }

        private class ArrayWithWarning
        {
            public readonly BlittableJsonReaderArray MergedArray;

            public ArrayWithWarning(BlittableJsonReaderArray mergedArray)
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
            public BlittableJsonReaderObject Document { get; set; }
            public BlittableJsonReaderObject Metadata { get; set; }
        }

        private static void WriteToken(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, string propertyName, object propertyValue)
        {
            writer.WritePropertyName(propertyName);
            if (propertyValue is BlittableJsonReaderObject.PropertyDetails)
            {
                var prop = (BlittableJsonReaderObject.PropertyDetails)propertyValue;
                writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                return;
            }

            var conflicted = propertyValue as Conflicted;
            if (conflicted != null)
            {
                writer.StartWriteArray();
                writer.WriteValue(">>>> conflict start");
                foreach (BlittableJsonReaderObject.PropertyDetails item in conflicted.Values)
                {
                    writer.WriteValue(item.Token & BlittableJsonReaderBase.TypesMask, item.Value);
                }
                writer.WriteValue("<<<< conflict end");
                writer.WriteArrayEnd();
                return;
            }

            var arrayWithWarning = propertyValue as ArrayWithWarning;
            if (arrayWithWarning != null)
            {
                writer.StartWriteArray();
                writer.WriteValue(">>>> auto merged array start");
                arrayWithWarning.MergedArray.AddItemsToStream(writer);
                writer.WriteValue("<<<< auto merged array end");
                writer.WriteArrayEnd();
                return;
            }

            var array = propertyValue as BlittableJsonReaderArray;
            if (array != null)
            {
                writer.StartWriteArray();
                array.AddItemsToStream(writer);
                writer.WriteArrayEnd();
                return;
            }

            throw new InvalidOperationException("Could not understand how to deal with: " + propertyValue);
        }

        private static void WriteConflictResolver(string name, ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> documentWriter,
            ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> metadataWriter, ConflictResolverAdvisor resolver, int indent)
        {
            MergeResult result = resolver.Resolve(indent);

            if (resolver.IsMetadataResolver)
            {
                if (name != Constants.Documents.Metadata.Key)
                {
                    metadataWriter.WritePropertyName(name);
                    metadataWriter.StartWriteObject();
                    result.Document.AddItemsToStream(metadataWriter);
                    metadataWriter.WriteObjectEnd();
                    return;
                }
                result.Document.AddItemsToStream(metadataWriter);
            }
            else
            {
                documentWriter.WritePropertyName(name);
                documentWriter.StartWriteObject();
                result.Document.AddItemsToStream(documentWriter);
                documentWriter.WriteObjectEnd();
            }
        }

        private MergeResult GenerateOutput(Dictionary<string, object> result, int indent)
        {
            using (var documentWriter = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(_context))
            using (var metadataWriter = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(_context))
            {
                documentWriter.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                metadataWriter.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                documentWriter.StartWriteObjectDocument();
                metadataWriter.StartWriteObjectDocument();
                documentWriter.StartWriteObject();
                metadataWriter.StartWriteObject();

                foreach (var o in result)
                {
                    var resolver = o.Value as ConflictResolverAdvisor;
                    if (resolver != null)
                    {
                        WriteConflictResolver(o.Key, documentWriter, metadataWriter, resolver,
                            o.Key == Constants.Documents.Metadata.Key ? 0 : indent + 1);
                    }
                    else
                    {
                        WriteToken(o.Key == Constants.Documents.Metadata.Key ? metadataWriter : documentWriter, o.Key, o.Value);
                    }
                }

                documentWriter.WriteObjectEnd();
                metadataWriter.WriteObjectEnd();
                documentWriter.FinalizeDocument();
                metadataWriter.FinalizeDocument();

                return new MergeResult
                {
                    Document = documentWriter.CreateReader(),
                    Metadata = metadataWriter.CreateReader()
                };
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Raven.Client;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using ServerWriterExtensions = Raven.Server.Json.BlittableJsonTextWriterExtensions;

namespace FastTests.Blittable
{
    public unsafe class BlittableJsonTextWriterOutput(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const int NumberOfOperations = 20_000;

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Writing_an_object_matches_writing_its_materialized_values()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                var random = new Random(1337);
                var count = 0;
                foreach (var document in CreateDocuments(context, random))
                {
                    var actual = Write(context, writer => writer.WriteObject(document));
                    var expected = Write(context, writer => WriteObjectFromMaterializedValues(writer, document));
                    AssertSameBytes(expected, actual, count);

                    hash.AppendData(actual);
                    count++;
                }

                Output.WriteLine($"{count} objects, output hash {Convert.ToHexString(hash.GetHashAndReset())}");
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Writing_a_document_matches_writing_its_materialized_values()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                var random = new Random(2027);
                var count = 0;
                Func<LazyStringValue, bool> filterCounters = name => name.Equals(Constants.Documents.Metadata.Counters);

                for (; count < 300; count++)
                {
                    var document = CreateDocument(context, random, count);
                    foreach (var metadataOnly in new[] { false, true })
                    {
                        foreach (var filter in new[] { null, filterCounters })
                        {
                            var actual = Write(context, writer => ServerWriterExtensions.WriteDocument(writer, context, document, metadataOnly, filter));
                            var expected = Write(context, writer => WriteDocumentFromMaterializedValues(writer, context, document, metadataOnly, filter));
                            AssertSameBytes(expected, actual, count);

                            hash.AppendData(actual);
                        }
                    }
                }

                Output.WriteLine($"{count} documents, output hash {Convert.ToHexString(hash.GetHashAndReset())}");
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Writing_a_string_matches_writing_it_as_a_lazy_string()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                var random = new Random(42);
                var strings = new List<string> { null, string.Empty, new string('x', 40_000), new string('"', 20_000) };
                strings.AddRange(TrickyStrings);
                for (int i = 0; i < 2_000; i++)
                    strings.Add(RandomString(random));
                for (int i = 0; i < 0x2000; i++)
                    strings.Add(((char)i).ToString());

                foreach (var str in strings)
                {
                    foreach (var skipEscaping in new[] { false, true })
                    {
                        var actual = Write(context, writer => writer.WriteString(str, skipEscaping));
                        var expected = Write(context, writer =>
                        {
                            using (var lazy = context.GetLazyString(str))
                                writer.WriteString(lazy, skipEscaping);
                        });
                        AssertSameBytes(expected, actual, str?.Length ?? -1);

                        hash.AppendData(actual);
                    }
                }

                Output.WriteLine($"{strings.Count} strings, output hash {Convert.ToHexString(hash.GetHashAndReset())}");
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData("{\"a\":\"\\u0001\",\"b\":\"\\n\",\"c\":\"\\u007f\",\"d\":\"x\\ny\",\"e\":\"\\\"\\\\\\/\\b\\f\\r\\t\"}",
            "{\"a\":\"\\u0001\",\"b\":\"\\u000A\",\"c\":\"\\u007F\",\"d\":\"x\\ny\",\"e\":\"\\\"\\\\/\\b\\f\\r\\t\"}")]
        [InlineData("{\"\\n\":1,\"\\\"q\\\"\":2,\"\\u05e9\\u05dc\\u05d5\\u05dd\":\"\\ud83d\\ude00\",\"\":[]}",
            "{\"\\u000A\":1,\"\\\"q\\\"\":2,\"שלום\":\"\uFFFD\uFFFD\",\"\":[]}")]
        [InlineData("{\"n\":[1.50,-0,0.0,1e10,1E+300,-1.5e-7,9223372036854775807,-9223372036854775808,123456789012345678901234567890,3.14159265358979323846264338327950288]}",
            "{\"n\":[1.50,0,0.0,1e10,1E+300,-1.5e-7,9223372036854775807,-9223372036854775808,123456789012345678901234567890,3.14159265358979323846264338327950288]}")]
        [InlineData("{\"e\":{},\"a\":[],\"aa\":[[],[[]],[{}],[null,true,false]],\"z\":{\"y\":{\"x\":{}}}}",
            "{\"e\":{},\"a\":[],\"aa\":[[],[[]],[{}],[null,true,false]],\"z\":{\"y\":{\"x\":{}}}}")]
        public void Writing_an_object_produces_the_expected_text(string json, string expected)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                foreach (var mode in new[] { BlittableJsonDocumentBuilder.UsageMode.None, BlittableJsonDocumentBuilder.UsageMode.CompressStrings | BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings })
                {
                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))
                    using (var document = JsonOperationSyncExtensions.ParseToMemory(context.Sync, stream, "doc", mode))
                    {
                        AssertText(expected, Write(context, writer => writer.WriteObject(document)));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Writing_lazy_numbers_and_vectors_produces_the_expected_text()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                builder.StartWriteObjectDocument();
                builder.StartWriteObject();
                builder.WritePropertyName("Numbers");
                builder.StartWriteArray();
                foreach (var number in new[] { "NaN", "Infinity", "-Infinity", "nan", "Inf", "-Inf", "NaNa", "Infinity1", "1E5", "-1e-5", "0.10" })
                    builder.WriteValue(new LazyNumberValue(context.GetLazyString(number)));
                builder.WriteValue(double.NaN);
                builder.WriteValue(double.PositiveInfinity);
                builder.WriteValue(double.NegativeInfinity);
                builder.WriteValue(-0.0);
                builder.WriteValue(1.5m);
                builder.WriteArrayEnd();
                builder.WritePropertyName("Floats");
                builder.WriteVector<float>(new[] { 1.5f, float.NaN, float.NegativeInfinity });
                builder.WritePropertyName("Longs");
                builder.WriteVector<long>(new[] { long.MinValue, 0, long.MaxValue });
                builder.WriteObjectEnd();
                builder.FinalizeDocument();

                using (var document = builder.CreateReader())
                {
                    AssertText("{\"Numbers\":[\"NaN\",\"Infinity\",\"-Infinity\",nan,Inf,-Inf,NaNa,Infinity1,1E5,-1e-5,0.10,\"NaN\",\"Infinity\",\"-Infinity\",-0.0,1.5]," +
                                 "\"Floats\":[1.5,\"NaN\",\"-Infinity\"],\"Longs\":[-9223372036854775808,0,9223372036854775807]}",
                        Write(context, writer => writer.WriteObject(document)));
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Writing_a_document_allocates_no_reader_objects()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var orders = new Document[64];
                for (int i = 0; i < orders.Length; i++)
                    orders[i] = CreateOrder(context, i);

                var writer = new AsyncBlittableJsonTextWriter(context, Stream.Null);
                try
                {
                    var i1 = 0;
                    var writeObject = MeasurePerOperation(() =>
                    {
                        writer.WriteObject(orders[i1++ % orders.Length].Data);
                        writer.MaybeFlushAsync().AsTask().GetAwaiter().GetResult();
                    });

                    var i2 = 0;
                    var writeDocument = MeasurePerOperation(() =>
                    {
                        ServerWriterExtensions.WriteDocument(writer, context, orders[i2++ % orders.Length], metadataOnly: false);
                        writer.MaybeFlushAsync().AsTask().GetAwaiter().GetResult();
                    });

                    var i3 = 0;
                    var writeMetadata = MeasurePerOperation(() =>
                    {
                        ServerWriterExtensions.WriteDocument(writer, context, orders[i3++ % orders.Length], metadataOnly: true);
                        writer.MaybeFlushAsync().AsTask().GetAwaiter().GetResult();
                    });

                    AssertAtMost(16, ("WriteObject, order", writeObject), ("WriteDocument, order", writeDocument), ("WriteDocument, order metadata", writeMetadata));
                }
                finally
                {
                    writer.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            }
        }

        private static Document CreateOrder(JsonOperationContext context, int i)
        {
            var lines = new DynamicJsonArray();
            for (int j = 0; j <= i % 8; j++)
            {
                lines.Add(new DynamicJsonValue
                {
                    ["Discount"] = j % 3 == 0 ? 0.15 : 0,
                    ["PricePerUnit"] = 14.5 + j,
                    ["Product"] = $"products/{11 + j}-A",
                    ["ProductName"] = "Queso Cabrales",
                    ["Quantity"] = 12 + j
                });
            }

            var data = context.ReadObject(new DynamicJsonValue
            {
                ["Company"] = $"companies/{85 + i}-A",
                ["Employee"] = "employees/5-A",
                ["Freight"] = 32.38 + i,
                ["IsPaid"] = i % 2 == 0,
                ["Lines"] = lines,
                ["Notes"] = "Leave at the back door, ring twice",
                ["OrderedAt"] = "1996-07-04T00:00:00.0000000",
                ["RequireAt"] = "1996-08-01T00:00:00.0000000",
                ["ShipTo"] = new DynamicJsonValue
                {
                    ["City"] = "Reims",
                    ["Country"] = "France",
                    ["Line1"] = "59 rue de l'Abbaye",
                    ["Line2"] = null,
                    ["Location"] = new DynamicJsonValue { ["Latitude"] = 49.2583, ["Longitude"] = 4.0317 },
                    ["PostalCode"] = "51100",
                    ["Region"] = null
                },
                ["ShipVia"] = "shippers/3-A",
                ["ShippedAt"] = "1996-07-16T00:00:00.0000000",
                ["Status"] = "Shipped",
                ["Total"] = 440.5 + i,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders",
                    [Constants.Documents.Metadata.RavenClrType] = "Orders.Order, Northwind"
                }
            }, $"orders/{i}-A");

            return new Document
            {
                Data = data,
                Id = context.GetLazyString($"orders/{i}-A"),
                ChangeVector = $"A:{1000 + i}-hSUUlkkaJUaqWHHGsnLbMw",
                LastModified = new DateTime(2026, 11, 23, 10, 0, i % 60, DateTimeKind.Utc),
                Flags = DocumentFlags.HasRevisions
            };
        }

        private void AssertAtMost(int limit, params (string Name, double BytesPerOperation)[] results)
        {
            foreach (var (name, bytesPerOperation) in results)
                Output.WriteLine($"{name,-32} {bytesPerOperation,8:F1} bytes/op");

#if !DEBUG
            foreach (var (name, bytesPerOperation) in results)
                Assert.True(bytesPerOperation <= limit, $"{name} allocated {bytesPerOperation:F1} bytes/op, expected at most {limit}");
#endif
        }

        private static double MeasurePerOperation(Action action)
        {
            for (int i = 0; i < 1_000; i++)
                action();

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < NumberOfOperations; i++)
                action();

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / NumberOfOperations;
        }

        private static byte[] Write(JsonOperationContext context, Action<AsyncBlittableJsonTextWriter> write)
        {
            using (var stream = new MemoryStream())
            {
                var writer = new AsyncBlittableJsonTextWriter(context, stream);
                try
                {
                    write(writer);
                    writer.FlushAsync().AsTask().GetAwaiter().GetResult();
                }
                finally
                {
                    writer.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }

                return stream.ToArray();
            }
        }

        private void AssertText(string expected, byte[] actual)
        {
            var text = Encoding.UTF8.GetString(actual);
            Output.WriteLine(text);
            Assert.Equal(expected, text);
        }

        private static void AssertSameBytes(byte[] expected, byte[] actual, int item)
        {
            if (expected.AsSpan().SequenceEqual(actual))
                return;

            var at = expected.AsSpan().CommonPrefixLength(actual);
            var from = Math.Max(0, at - 40);
            Assert.Fail($"Item {item} differs at byte {at}: expected ...{Excerpt(expected, from)}..., actual ...{Excerpt(actual, from)}...");

            static string Excerpt(byte[] bytes, int from) => Encoding.UTF8.GetString(bytes, from, Math.Min(120, bytes.Length - from));
        }

        // The reference output: every value is materialized through the reader API and written with the writer's primitives.
        private static void WriteObjectFromMaterializedValues(AbstractBlittableJsonTextWriter writer, BlittableJsonReaderObject obj)
        {
            if (obj == null)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteStartObject();
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var order = obj.GetPropertiesByInsertionOrder())
            {
                for (int i = 0; i < order.Size; i++)
                {
                    if (i != 0)
                        writer.WriteComma();

                    obj.GetPropertyByIndex(order.Properties[i], ref prop);
                    writer.WritePropertyName(prop.Name);
                    WriteMaterializedValue(writer, prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            writer.WriteEndObject();
        }

        private static void WriteMaterializedValue(AbstractBlittableJsonTextWriter writer, BlittableJsonToken token, object value)
        {
            switch (token)
            {
                case BlittableJsonToken.String:
                    writer.WriteString((LazyStringValue)value);
                    break;
                case BlittableJsonToken.CompressedString:
                    writer.WriteString((LazyCompressedStringValue)value);
                    break;
                case BlittableJsonToken.Integer:
                    writer.WriteInteger((long)value);
                    break;
                case BlittableJsonToken.LazyNumber:
                    writer.WriteDouble((LazyNumberValue)value);
                    break;
                case BlittableJsonToken.Boolean:
                    writer.WriteBool((bool)value);
                    break;
                case BlittableJsonToken.Null:
                    writer.WriteNull();
                    break;
                case BlittableJsonToken.StartObject:
                case BlittableJsonToken.EmbeddedBlittable:
                    WriteObjectFromMaterializedValues(writer, (BlittableJsonReaderObject)value);
                    break;
                case BlittableJsonToken.StartArray:
                    var array = (BlittableJsonReaderArray)value;
                    writer.WriteStartArray();
                    for (int i = 0; i < array.Length; i++)
                    {
                        if (i != 0)
                            writer.WriteComma();

                        var (item, itemToken) = array.GetValueTokenTupleByIndex(i);
                        WriteMaterializedValue(writer, itemToken, item);
                    }
                    writer.WriteEndArray();
                    break;
                default:
                    writer.WriteValue(token, value);
                    break;
            }
        }

        private static void WriteDocumentFromMaterializedValues(AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document,
            bool metadataOnly, Func<LazyStringValue, bool> filter)
        {
            writer.WriteStartObject();
            BlittableJsonReaderObject metadata;
            if (metadataOnly)
            {
                document.Data.TryGet(Constants.Documents.Metadata.Key, out metadata);
            }
            else
            {
                metadata = WritePropertiesFromMaterializedValues(writer, context, document.Data);
            }

            writer.WritePropertyName(Constants.Documents.Metadata.Key);
            writer.WriteStartObject();
            var first = true;
            if (metadata != null)
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();
                for (int i = 0; i < metadata.Count; i++)
                {
                    metadata.GetPropertyByIndex(i, ref prop);
                    if (filter != null && filter(prop.Name))
                        continue;

                    if (first == false)
                        writer.WriteComma();
                    first = false;
                    writer.WritePropertyName(prop.Name);
                    WriteMaterializedValue(writer, prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
                writer.WriteComma();
            writer.WritePropertyName(Constants.Documents.Metadata.ChangeVector);
            WriteStringAsLazyString(writer, context, document.ChangeVector);

            if (document.Flags != DocumentFlags.None)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.Flags);
                WriteStringAsLazyString(writer, context, document.Flags.ToString());
            }
            if (document.Id != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.Id);
                writer.WriteString(document.Id);
            }
            if (document.IndexScore != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.IndexScore);
                writer.WriteDouble(document.IndexScore.Value);
            }
            if (document.Distance != null)
            {
                writer.WriteComma();
                var result = document.Distance.Value;
                writer.WritePropertyName(Constants.Documents.Metadata.SpatialResult);
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(result.Distance));
                writer.WriteDouble(result.Distance);
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.Latitude));
                writer.WriteDouble(result.Latitude);
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.Longitude));
                writer.WriteDouble(result.Longitude);
                writer.WriteEndObject();
            }
            if (document.LastModified != DateTime.MinValue)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.LastModified);
                writer.WriteDateTime(document.LastModified, isUtc: true);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        private static BlittableJsonReaderObject WritePropertiesFromMaterializedValues(AbstractBlittableJsonTextWriter writer, JsonOperationContext context,
            BlittableJsonReaderObject data)
        {
            BlittableJsonReaderObject metadata = null;
            var metadataField = context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Key);
            var first = true;
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var order = data.GetPropertiesByInsertionOrder())
            {
                for (int i = 0; i < order.Size; i++)
                {
                    data.GetPropertyByIndex(order.Properties[i], ref prop);
                    if (metadataField.Equals(prop.Name))
                    {
                        metadata = (BlittableJsonReaderObject)prop.Value;
                        continue;
                    }

                    if (first == false)
                        writer.WriteComma();
                    first = false;
                    writer.WritePropertyName(prop.Name);
                    WriteMaterializedValue(writer, prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
                writer.WriteComma();

            return metadata;
        }

        private static void WriteStringAsLazyString(AbstractBlittableJsonTextWriter writer, JsonOperationContext context, string str)
        {
            using (var lazy = context.GetLazyString(str))
                writer.WriteString(lazy);
        }

        private static readonly string[] Words =
        {
            "", "a", "Reims", "59 rue de l'Abbaye", "products/11-A", "Queso Cabrales", "שלום עולם", "日本語のテキスト", "emoji 😀🎉", "ünïcödé mixed",
            "@metadata", "Raven-Clr-Type", "1996-07-04T00:00:00.0000000"
        };

        private static readonly string[] TrickyStrings =
        {
            "\"", "\\", "/", "\"quoted\"", "back\\slash", "slash/es", "tab\there", "line\nfeed", "cr\rlf\n", "\b\f\v", "\u0000", "\u0001", "\u001F", "\u007F",
            "\n", "\t", "x\u0000y", "\u2028\u2029", "\uD83D", "\uDE00 lone low", "end\\", "\u0080", "\u009F", "\u00A0", "\uFFFF", "\"\"\"\"", "\\\\\\\\"
        };

        private static readonly string[] NumberLiterals =
        {
            "0", "-0", "1.50", "0.0", "1e10", "1E+300", "-1.5e-7", "123456789012345678901234567890", "3.14159265358979323846264338327950288", "-0.000001",
            "1.0E-5", "NaN", "Infinity", "-Infinity", "nan", "Inf", "-Inf", "NaNa", "Infinity1", "-Infinityx", "1", "-"
        };

        private static string RandomString(Random random)
        {
            switch (random.Next(10))
            {
                case 0:
                case 1:
                case 2:
                    return Words[random.Next(Words.Length)];
                case 3:
                case 4:
                    return TrickyStrings[random.Next(TrickyStrings.Length)];
                case 5:
                case 6:
                    var sb = new StringBuilder();
                    for (int i = random.Next(1, 6); i > 0; i--)
                        sb.Append(random.Next(2) == 0 ? Words[random.Next(Words.Length)] : TrickyStrings[random.Next(TrickyStrings.Length)]);
                    return sb.ToString();
                case 7:
                    return ((char)random.Next(0, 0xA0)).ToString();
                default:
                    var length = random.Next(20) == 0 ? random.Next(32_000, 70_000) : random.Next(100, 3_000);
                    var builder = new StringBuilder(length);
                    while (builder.Length < length)
                        builder.Append(random.Next(4) == 0 ? TrickyStrings[random.Next(TrickyStrings.Length)] : Words[random.Next(Words.Length)]);
                    return builder.ToString();
            }
        }

        private sealed class Obj : List<(string Name, object Value)>
        {
        }

        private sealed class Blob(byte[] bytes)
        {
            public readonly byte[] Bytes = bytes;
        }

        private static IEnumerable<BlittableJsonReaderObject> CreateDocuments(JsonOperationContext context, Random random)
        {
            var modes = new[]
            {
                BlittableJsonDocumentBuilder.UsageMode.None,
                BlittableJsonDocumentBuilder.UsageMode.CompressStrings,
                BlittableJsonDocumentBuilder.UsageMode.CompressStrings | BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings
            };

            for (int i = 0; i < 300; i++)
                yield return Build(context, RandomObject(context, random, depth: 0), modes[i % modes.Length]);

            foreach (var mode in modes)
            {
                for (int i = 0; i < 40; i++)
                {
                    var json = new StringBuilder("{\"n\":[");
                    for (int j = random.Next(1, 30); j > 0; j--)
                    {
                        json.Append(NumberLiterals[random.Next(11)]);
                        if (j > 1)
                            json.Append(',');
                    }
                    json.Append("],\"s\":\"\\/\\u00e9\\ud83d\\ude00\\u0000\\\"\\\\").Append(new string('z', random.Next(300))).Append("\"}");

                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json.ToString())))
                        yield return JsonOperationSyncExtensions.ParseToMemory(context.Sync, stream, "doc", mode);
                }
            }
        }

        private static BlittableJsonReaderObject Build(JsonOperationContext context, Obj root, BlittableJsonDocumentBuilder.UsageMode mode)
        {
            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                builder.Reset(mode);
                builder.StartWriteObjectDocument();
                Emit(context, builder, root);
                builder.FinalizeDocument();
                return builder.CreateReader();
            }
        }

        private static void Emit(JsonOperationContext context, ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> builder, object value)
        {
            switch (value)
            {
                case null:
                    builder.WriteValueNull();
                    break;
                case bool b:
                    builder.WriteValue(b);
                    break;
                case long l:
                    builder.WriteValue(l);
                    break;
                case ulong ul:
                    builder.WriteValue(ul);
                    break;
                case double d:
                    builder.WriteValue(d);
                    break;
                case float f:
                    builder.WriteValue(f);
                    break;
                case decimal m:
                    builder.WriteValue(m);
                    break;
                case LazyNumberValue lnv:
                    builder.WriteValue(lnv);
                    break;
                case string s:
                    builder.WriteValue(s);
                    break;
                case Obj obj:
                    builder.StartWriteObject();
                    foreach (var (name, propertyValue) in obj)
                    {
                        builder.WritePropertyName(name);
                        Emit(context, builder, propertyValue);
                    }
                    builder.WriteObjectEnd();
                    break;
                case List<object> array:
                    builder.StartWriteArray();
                    foreach (var item in array)
                        Emit(context, builder, item);
                    builder.WriteArrayEnd();
                    break;
                case BlittableJsonReaderObject embedded:
                    builder.WriteEmbeddedBlittableDocument(embedded);
                    break;
                case Blob blob:
                    var memory = context.GetMemory(Math.Max(1, blob.Bytes.Length));
                    blob.Bytes.CopyTo(new Span<byte>(memory.Address, blob.Bytes.Length));
                    builder.WriteRawBlob(memory.Address, blob.Bytes.Length);
                    break;
                case int[] ints:
                    builder.WriteVector<int>(ints);
                    break;
                case double[] doubles:
                    builder.WriteVector<double>(doubles);
                    break;
                case float[] floats:
                    builder.WriteVector<float>(floats);
                    break;
                case sbyte[] sbytes:
                    builder.WriteVector<sbyte>(sbytes);
                    break;
                case ulong[] ulongs:
                    builder.WriteVector<ulong>(ulongs);
                    break;
                default:
                    throw new InvalidOperationException("Unexpected value " + value.GetType());
            }
        }

        private static Obj RandomObject(JsonOperationContext context, Random random, int depth)
        {
            var obj = new Obj();
            var names = new HashSet<string>();
            var count = random.Next(10) == 0 ? 0 : random.Next(1, depth == 0 ? 40 : 12);
            for (int i = 0; i < count; i++)
            {
                var name = random.Next(3) == 0 ? RandomString(random) : Words[random.Next(Words.Length)] + i;
                if (name.Length > 200 || names.Add(name) == false)
                    continue;

                obj.Add((name, RandomValue(context, random, depth)));
            }

            return obj;
        }

        private static object RandomValue(JsonOperationContext context, Random random, int depth)
        {
            switch (random.Next(depth >= 4 ? 13 : 20))
            {
                case 0:
                    return null;
                case 1:
                    return random.Next(2) == 0;
                case 2:
                    return random.Next(4) switch
                    {
                        0 => long.MinValue,
                        1 => long.MaxValue,
                        2 => 0L,
                        _ => (long)random.Next() * random.Next(-1000, 1000)
                    };
                case 3:
                    return random.Next(8) switch
                    {
                        0 => double.NaN,
                        1 => double.PositiveInfinity,
                        2 => double.NegativeInfinity,
                        3 => double.Epsilon,
                        4 => -0.0,
                        5 => double.MaxValue,
                        _ => (random.NextDouble() - 0.5) * Math.Pow(10, random.Next(-30, 30))
                    };
                case 4:
                    return random.Next(3) switch
                    {
                        0 => (decimal)random.Next() / 1000m,
                        1 => decimal.MaxValue,
                        _ => -79228162514264337593543950335m / (random.Next(1, 100))
                    };
                case 5:
                    return random.Next(3) switch
                    {
                        0 => (object)(float)random.NextDouble(),
                        1 => (object)ulong.MaxValue,
                        _ => (object)new LazyNumberValue(context.GetLazyString(NumberLiterals[random.Next(NumberLiterals.Length)]))
                    };
                case 6:
                case 7:
                case 8:
                case 9:
                case 10:
                case 11:
                case 12:
                    return RandomString(random);
                case 13:
                case 14:
                case 15:
                    return RandomObject(context, random, depth + 1);
                case 16:
                case 17:
                    var array = new List<object>();
                    for (int i = random.Next(10) == 0 ? 0 : random.Next(1, 10); i > 0; i--)
                        array.Add(RandomValue(context, random, depth + 1));
                    return array;
                case 18:
                    return random.Next(3) switch
                    {
                        0 => Build(context, RandomObject(context, random, depth + 2), BlittableJsonDocumentBuilder.UsageMode.CompressStrings),
                        1 => new Blob(RandomBytes(random)),
                        _ => new List<object> { new Blob(RandomBytes(random)), Build(context, RandomObject(context, random, depth + 2), BlittableJsonDocumentBuilder.UsageMode.None) }
                    };
                default:
                    return random.Next(5) switch
                    {
                        0 => new[] { 1, -2, int.MaxValue, int.MinValue },
                        1 => new[] { 0.5, double.NaN, double.PositiveInfinity, -1e300 },
                        2 => new[] { 1.25f, float.NegativeInfinity, -0.0f },
                        3 => new sbyte[] { -128, 0, 127 },
                        _ => new[] { ulong.MaxValue, 0UL }
                    };
            }
        }

        private static byte[] RandomBytes(Random random)
        {
            var bytes = new byte[random.Next(0, 64)];
            random.NextBytes(bytes);
            return bytes;
        }

        private static Document CreateDocument(JsonOperationContext context, Random random, int index)
        {
            var root = RandomObject(context, random, depth: 0);
            root.RemoveAll(x => x.Name == Constants.Documents.Metadata.Key);

            object metadata = random.Next(6) switch
            {
                0 => null,
                1 => Build(context, RandomMetadata(context, random), BlittableJsonDocumentBuilder.UsageMode.None),
                _ => RandomMetadata(context, random)
            };

            if (random.Next(8) != 0)
                root.Insert(random.Next(root.Count + 1), (Constants.Documents.Metadata.Key, metadata));

            return new Document
            {
                Data = Build(context, root, BlittableJsonDocumentBuilder.UsageMode.CompressStrings),
                Id = random.Next(5) == 0 ? null : context.GetLazyString("orders/" + index + RandomString(random)),
                ChangeVector = random.Next(5) == 0 ? null : $"A:{index}-hSUUlkkaJUaqWHHGsnLbMw, B:{random.Next()}-{RandomString(random)}",
                Flags = (DocumentFlags)(random.Next(4) == 0 ? 0 : random.Next(0, 0x40000)),
                IndexScore = random.Next(3) == 0 ? (float)random.NextDouble() : null,
                Distance = random.Next(4) == 0 ? new SpatialResult { Distance = random.NextDouble(), Latitude = double.NaN, Longitude = -random.NextDouble() } : null,
                LastModified = random.Next(4) == 0 ? DateTime.MinValue : new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(random.Next())
            };
        }

        private static Obj RandomMetadata(JsonOperationContext context, Random random)
        {
            var metadata = new Obj
            {
                (Constants.Documents.Metadata.Collection, RandomString(random)),
                (Constants.Documents.Metadata.RavenClrType, "Orders.Order, Northwind"),
                (Constants.Documents.Metadata.Counters, new List<object> { "likes", "dislikes" }),
                (Constants.Documents.Metadata.Attachments, new List<object> { new Obj { ("Name", "a.png"), ("Size", 123L), ("ContentType", "image/png") } })
            };

            if (random.Next(2) == 0)
                metadata.Add((Constants.Documents.Metadata.Flags, "HasRevisions, HasCounters"));
            if (random.Next(2) == 0)
                metadata.Add(("Extra", RandomValue(context, random, 2)));

            return metadata;
        }
    }
}

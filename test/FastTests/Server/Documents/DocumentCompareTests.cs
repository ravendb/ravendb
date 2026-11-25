using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents
{
    public class DocumentCompareTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("{\"A\":1,\"B\":\"x\"}", "{\"A\":1,\"B\":\"x\"}", true)]
        [InlineData("{\"A\":1,\"B\":\"x\"}", "{\"B\":\"x\",\"A\":1}", true)]
        [InlineData("{\"A\":1,\"B\":\"x\"}", "{\"A\":2,\"B\":\"x\"}", false)]
        [InlineData("{\"A\":1}", "{\"A\":1,\"B\":\"x\"}", false)]
        [InlineData("{\"A\":1,\"B\":\"x\"}", "{\"A\":1}", false)]
        [InlineData("{\"A\":null}", "{}", false)]
        [InlineData("{\"A\":null}", "{\"A\":null}", true)]
        [InlineData("{\"@x\":1,\"A\":1}", "{\"@x\":1,\"A\":1}", true)]
        [InlineData("{\"@x\":1,\"A\":1}", "{\"@x\":2,\"A\":1}", false)]
        [InlineData("{\"`metadata\":1}", "{}", false)]
        [InlineData("{\"A\":{\"X\":[1,2,{\"Y\":\"z\"}]}}", "{\"A\":{\"X\":[1,2,{\"Y\":\"z\"}]}}", true)]
        [InlineData("{\"A\":{\"X\":[1,2,{\"Y\":\"z\"}]}}", "{\"A\":{\"X\":[1,2,{\"Y\":\"w\"}]}}", false)]
        [InlineData("{\"\\u05e9\\u05dd\":\"\\\"q\\\"\",\"A\":1.5}", "{\"A\":1.5,\"\\u05e9\\u05dd\":\"\\\"q\\\"\"}", true)]
        [InlineData("{\"Name\":1}", "{\"name\":1}", false)]
        public void Compares_document_bodies(string current, string modified, bool equal)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var a = Read(context, current))
            using (var b = Read(context, modified))
            {
                var expected = equal ? DocumentCompareResult.Equal : DocumentCompareResult.NotEqual;
                Assert.Equal(expected, DocumentCompare.IsEqualTo(a, b, DocumentCompare.DocumentCompareOptions.Default));
                Assert.Equal(expected, DocumentCompare.IsEqualTo(b, a, DocumentCompare.DocumentCompareOptions.Default));
            }
        }

        private const string AttachmentA = "{\"Name\":\"a\",\"Hash\":\"h1\",\"ContentType\":\"\",\"Size\":1}";
        private const string AttachmentB = "{\"Name\":\"b\",\"Hash\":\"h2\",\"ContentType\":\"\",\"Size\":2}";

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("{\"@collection\":\"A\"}", "{\"@collection\":\"A\"}", 0, "Equal")]
        [InlineData("{\"@collection\":\"A\"}", "{\"@collection\":\"B\"}", 0, "NotEqual")]
        [InlineData("{\"@collection\":\"A\"}", "{\"@COLLECTION\":\"A\"}", 0, "NotEqual")]
        [InlineData("{\"@collection\":\"A\",\"@flags\":\"X\"}", "{\"@collection\":\"A\",\"@flags\":\"Y\"}", 0, "Equal")]
        [InlineData("{\"@collection\":\"A\",\"@flags\":\"X\"}", "{\"@collection\":\"A\"}", 0, "Equal")]
        [InlineData("{\"@collection\":\"A\",\"Custom\":1}", "{\"@collection\":\"A\",\"Custom\":2}", 0, "NotEqual")]
        [InlineData("{\"@collection\":\"A\",\"@expires\":\"2030\"}", "{\"@collection\":\"A\"}", 0, "NotEqual")]
        [InlineData("{\"@archive-at\":\"2030\"}", "{}", 0, "Equal")]
        [InlineData("{\"@archive-at\":\"2030\"}", "{}", 3, "NotEqual")]
        [InlineData("{\"@counters\":[\"a\"]}", "{}", 1, "Equal, CountersNotEqual")]
        [InlineData("{\"@counters\":[\"a\"]}", "{\"@counters\":[\"b\"]}", 1, "Equal, CountersNotEqual")]
        [InlineData("{\"@counters\":[\"a\"]}", "{\"@counters\":[\"a\"]}", 1, "Equal")]
        [InlineData("{\"@counters\":[\"a\"]}", "{}", 0, "NotEqual")]
        [InlineData("{\"@timeseries\":[\"a\"]}", "{}", 1, "Equal, TimeSeriesNotEqual")]
        [InlineData("{\"@attachments\":[" + AttachmentA + "]}", "{}", 2, "Equal, AttachmentsNotEqual")]
        [InlineData("{\"@attachments\":[" + AttachmentA + "]}", "{\"@attachments\":[" + AttachmentA + "]}", 2, "Equal")] // same bytes
        [InlineData("{\"@attachments\":[" + AttachmentA + "],\"@flags\":\"X\"}", "{\"@attachments\":[" + AttachmentA + "],\"@flags\":\"Y\"}", 2, "Equal, AttachmentsNotEqual")]
        [InlineData("{\"@attachments\":[" + AttachmentA + "]}", "{\"@attachments\":[" + AttachmentB + "]}", 2, "throws")]
        [InlineData("{\"@collection\":\"A\"}", "{\"@collection\":\"A\",\"Custom\":1}", 0, "NotEqual")]
        [InlineData("{\"@Collection\":\"A\",\"@x\":1}", "{\"@COLLECTION\":\"A\",\"@x\":2}", 0, "Equal")] // names are matched exactly, these are unknown metadata properties
        public void Compares_metadata(string currentMetadata, string modifiedMetadata, int options, string expected)
        {
            var compareOptions = options switch
            {
                0 => DocumentCompare.DocumentCompareOptions.Default,
                1 => DocumentCompare.DocumentCompareOptions.MergeMetadata,
                2 => DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModification,
                _ => DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModificationCompareDataArchivalMetadata
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var a = Read(context, "{\"A\":1,\"@metadata\":" + currentMetadata + "}"))
            using (var b = Read(context, "{\"A\":1,\"@metadata\":" + modifiedMetadata + "}"))
            {
                if (expected == "throws")
                {
                    Assert.Throws<InvalidOperationException>(() => DocumentCompare.IsEqualTo(a, b, compareOptions));
                    return;
                }

                Assert.Equal(expected, DocumentCompare.IsEqualTo(a, b, compareOptions).ToString());
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Body_comparison_matches_comparing_by_property_name_strings()
        {
            var random = new Random(27880);
            // only "@metadata" is the metadata key, "@METADATA" and "@MetaData" are ordinary properties
            string[] names = ["A", "B", "Cc", "@x", "@METADATA", "@MetaData", "\u05e9", "z9"];
            object[] values = [1, 2, "x", "y", null, 1.5, true];

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 2_000; i++)
                {
                    using (var a = Read(context, RandomDocument(random, names, values)))
                    using (var b = Read(context, random.Next(4) == 0 ? RandomDocument(random, names, values) : Mutate(random, a, names, values)))
                    {
                        var expected = ReferenceBodiesEqual(a, b) ? DocumentCompareResult.Equal : DocumentCompareResult.NotEqual;
                        Assert.True(expected == DocumentCompare.IsEqualTo(a, b, DocumentCompare.DocumentCompareOptions.Default), $"{a} vs {b}");
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Comparing_an_updated_document_allocates_no_name_strings()
        {
            const string order = "{\"Company\":\"companies/17\",\"Employee\":\"employees/3\",\"OrderedAt\":\"2024-05-01T10:00:00.0000000Z\"," +
                                 "\"RequireAt\":\"2024-05-15T10:00:00.0000000Z\",\"ShippedAt\":null,\"ShipVia\":\"shippers/2\",\"Freight\":32.38," +
                                 "\"ShipTo\":{\"Line1\":\"12 alpha st.\",\"City\":\"lima\",\"PostalCode\":\"51100\",\"Country\":\"Israel\"}," +
                                 "\"Lines\":[{\"Product\":\"products/11\",\"Quantity\":12}],\"@metadata\":{\"@collection\":\"Orders\"}}";

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var current = Read(context, order))
            using (var modified = Read(context, order.Replace("32.38", "33.38")))
            {
                for (int i = 0; i < 1_000; i++)
                    DocumentCompare.IsEqualTo(current, modified, DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModification);

                const int count = 10_000;
                var before = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < count; i++)
                    Assert.Equal(DocumentCompareResult.NotEqual, DocumentCompare.IsEqualTo(current, modified, DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModification));
                var bytes = (double)(GC.GetAllocatedBytesForCurrentThread() - before) / count;

                Output.WriteLine($"IsEqualTo, one changed property {bytes,8:F1} bytes/op");
#if !DEBUG
                Assert.True(bytes <= 1_800, $"IsEqualTo allocated {bytes:F1} bytes/op, expected at most 1,800");
#endif
            }
        }

        private static BlittableJsonReaderObject Read(JsonOperationContext context, string json) => context.Sync.ReadForMemory(json, "doc");

        // the comparison of a document body as it was done with property name strings
        private static bool ReferenceBodiesEqual(BlittableJsonReaderObject current, BlittableJsonReaderObject modified)
        {
            var properties = new HashSet<string>(current.GetPropertyNames());
            foreach (var propertyName in modified.GetPropertyNames())
                properties.Add(propertyName);

            foreach (var property in properties)
            {
                if (property == Constants.Documents.Metadata.Key)
                    continue;

                if (current.TryGetMember(property, out object currentProperty) == false ||
                    modified.TryGetMember(property, out object modifiedProperty) == false)
                    return false;

                if (Equals(currentProperty, modifiedProperty) == false)
                    return false;
            }

            return true;
        }

        private static string RandomDocument(Random random, string[] names, object[] values)
        {
            var properties = new List<(string Name, string Value)>();
            int count = random.Next(0, names.Length);
            for (int i = 0; i < count; i++)
            {
                var name = names[random.Next(names.Length)];
                if (properties.Exists(p => p.Name == name) == false)
                    properties.Add((name, ToJson(values[random.Next(values.Length)])));
            }

            return ToJsonObject(properties);
        }

        private static string Mutate(Random random, BlittableJsonReaderObject source, string[] names, object[] values)
        {
            var properties = new List<(string Name, string Value)>();
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < source.Count; i++)
            {
                source.GetPropertyByIndex(i, ref prop);
                properties.Add((prop.Name.ToString(), ToJson(prop.Value)));
            }

            switch (random.Next(4))
            {
                case 0 when properties.Count > 0:
                    properties.RemoveAt(random.Next(properties.Count));
                    break;
                case 1 when properties.Count > 0:
                    var index = random.Next(properties.Count);
                    properties[index] = (properties[index].Name, ToJson(values[random.Next(values.Length)]));
                    break;
                case 2:
                    var name = names[random.Next(names.Length)];
                    if (properties.Exists(p => p.Name == name) == false)
                        properties.Add((name, ToJson(values[random.Next(values.Length)])));
                    break;
            }

            // shuffled, so the property order differs from the source
            for (int i = properties.Count - 1; i > 0; i--)
            {
                int j = random.Next(i + 1);
                (properties[i], properties[j]) = (properties[j], properties[i]);
            }

            return ToJsonObject(properties);
        }

        private static string ToJsonObject(List<(string Name, string Value)> properties)
        {
            var sb = new StringBuilder("{");
            foreach (var (name, value) in properties)
            {
                if (sb.Length > 1)
                    sb.Append(',');
                sb.Append('"').Append(name).Append("\":").Append(value);
            }

            return sb.Append('}').ToString();
        }

        private static string ToJson(object value) => value switch
        {
            null => "null",
            bool b => b ? "true" : "false",
            string s => $"\"{s}\"",
            LazyStringValue s => $"\"{s}\"",
            double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            LazyNumberValue n => n.ToString(),
            _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)
        };
    }
}

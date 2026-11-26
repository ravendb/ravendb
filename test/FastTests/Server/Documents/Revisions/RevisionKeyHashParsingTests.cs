using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Revisions;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Server.Documents.Revisions
{
    // The revision key hash parses the change vector bytes with ChangeVectorParser.ParseUtf8. The keys are stored, so they must
    // not change, and ParseUtf8 must read the same entries and reject the same versions as ChangeVectorParser.ToChangeVectorList.
    public class RevisionKeyHashParsingTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const string DbId1 = "Q2DQ7XoZ6kyk1q9bHoCqvw";
        private const string DbId2 = "zPm1Bgl3DE6JtqZGfJqRww";
        private const string DbId3 = "7l+0sJ4gZkKdTV2v8pS0Ag";

        // the keys that hashing the entries of ToChangeVectorList gave
        public static TheoryData<string, string> StoredKeys() => new()
        {
            { $"A:1-{DbId1}", "AAGX4i9b4wrMcH8hpj2CTB3R" },
            { $"B:20-{DbId2}, A:5-{DbId1}", "ABmZXXdMdN6gFX6kuRtDH5My" },
            { $"A:5-{DbId1}, A:3-{DbId1}", "AAguyzdYql58o6pPHfUyuDV9" },
            { $"RAFT:7-{DbId3}, TRXN:3-{DbId2}, SINK:9223372036854775807-{DbId1}", "AAlHgfn9bu8MG253V6MyqX+V" },
            { $"A:1-{DbId1} ,B:2-{DbId2}  ", "AAOI+s4UQLnxbOq2pDyfAe4a" },
            { FortyEntries(), "5uB7v8csmnmzCPXg2aeoP3mN" }
        };

        [RavenTheory(RavenTestCategory.Revisions)]
        [MemberData(nameof(StoredKeys))]
        public void Revision_keys_do_not_change(string version, string key)
        {
            AssertParsesLikeTheStringParser(version);
            Assert.Equal(key, Compute(version, strict: true));
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public void Utf8_parser_reads_the_entries_of_the_string_parser()
        {
            var random = new Random(27880);
            var dbIds = new List<string>();
            for (int i = 0; i < 12; i++)
                dbIds.Add(RandomDbId(random));
            string[] tags = ["A", "B", "Z", "AB", "SINK", "TRXN", "RAFT", "MOVE"];
            string[] separators = [", ", ",", " ", " , ", ",\t", "\n"];

            for (int i = 0; i < 5_000; i++)
            {
                var sb = new StringBuilder();
                int count = random.Next(1, 12);
                for (int j = 0; j < count; j++)
                {
                    if (j > 0)
                        sb.Append(separators[random.Next(separators.Length)]);
                    long etag = random.Next(4) switch
                    {
                        0 => random.Next(0, 10),
                        1 => random.Next(),
                        2 => random.NextInt64(),
                        _ => long.MaxValue
                    };
                    sb.Append(tags[random.Next(tags.Length)]).Append(':').Append(etag).Append('-').Append(dbIds[random.Next(dbIds.Count)]);
                }

                AssertParsesLikeTheStringParser(sb.ToString());
            }
        }

        [RavenTheory(RavenTestCategory.Revisions)]
        [InlineData(" ")]
        [InlineData("A:1")]
        [InlineData("A:1-")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAA")] // 21 characters
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAAA")]
        [InlineData(":1-AAAAAAAAAAAAAAAAAAAAAA")]
        [InlineData("a:1-AAAAAAAAAAAAAAAAAAAAAA")]
        [InlineData("A:-AAAAAAAAAAAAAAAAAAAAAA")]
        [InlineData("A:1x2-AAAAAAAAAAAAAAAAAAAAAA")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAAAx")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAAA,")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAAA ,B:2-BBBBBBBBBBBBBBBBBBBBBB  ")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAAA, B")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAAAB:2-BBBBBBBBBBBBBBBBBBBBBB")]
        [InlineData("A:99999999999999999999-AAAAAAAAAAAAAAAAAAAAAA")]
        [InlineData("Á:1-AAAAAAAAAAAAAAAAAAAAAA")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAé")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAAé")]
        [InlineData("A:1é-AAAAAAAAAAAAAAAAAAAAAA")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAAA,\u00A0B:2-BBBBBBBBBBBBBBBBBBBBBB")]
        [InlineData("A:1-AAAAAAAAAAAAAAAAAAAAA\uD800")] // a lone surrogate, encoded as U+FFFD
        public void Unusual_versions_are_handled_like_the_string_parser(string version)
        {
            var parsed = AssertParsesLikeTheStringParser(version);

            // strict throws what the parser throws, otherwise a version the parser rejects gives an empty key
            var strict = Compute(version, strict: true);
            var lenient = Compute(version, strict: false);
            if (parsed.EndsWith("Exception"))
            {
                Assert.Equal(parsed, strict);
                Assert.Equal("<empty>", lenient);
            }
            else
            {
                Assert.Equal(strict, lenient);
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public void An_empty_version_has_no_key()
        {
            Assert.Equal(typeof(ArgumentException).FullName, Compute("", strict: true));
            Assert.Equal("<empty>", Compute("", strict: false));
        }

        // the bytes are decoded like Encoding.UTF8.GetString decodes them, an invalid sequence becomes U+FFFD
        [RavenFact(RavenTestCategory.Revisions)]
        public void Invalid_utf8_is_decoded_like_the_string_parser()
        {
            var bytes = Encoding.UTF8.GetBytes("A:1-" + new string('A', 21)).Concat(new byte[] { 0xFF }).ToArray();
            AssertParsesLikeTheStringParser(bytes);

            bytes = Encoding.UTF8.GetBytes("A:1-" + new string('A', 20)).Concat(new byte[] { 0xC3, 0x28 }).ToArray();
            AssertParsesLikeTheStringParser(bytes);
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public void Hashing_a_version_allocates_nothing()
        {
            var version = $"A:1234-{RandomDbId(new Random(2))}, B:99-{RandomDbId(new Random(3))}, C:7-{RandomDbId(new Random(4))}";
            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            using (Slice.From(allocator, version, out Slice raw))
            {
                for (int i = 0; i < 1_000; i++)
                {
                    using (RevisionsStorage.GetRevisionKeyHashSlice(allocator, raw, out _))
                    {
                    }
                }

                const int count = 10_000;
                var before = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < count; i++)
                {
                    using (RevisionsStorage.GetRevisionKeyHashSlice(allocator, raw, out _))
                    {
                    }
                }

                var bytes = (double)(GC.GetAllocatedBytesForCurrentThread() - before) / count;
                Output.WriteLine($"GetRevisionKeyHashSlice {bytes:F1} bytes/op");
#if !DEBUG
                Assert.True(bytes <= 8, $"GetRevisionKeyHashSlice allocated {bytes:F1} bytes/op");
#endif
            }
        }

        // the entries both parsers read, or the type of the exception they throw
        private static string AssertParsesLikeTheStringParser(string version) => AssertParsesLikeTheStringParser(Encoding.UTF8.GetBytes(version));

        private static string AssertParsesLikeTheStringParser(byte[] utf8)
        {
            var version = Encoding.UTF8.GetString(utf8);
            var expected = Describe(() => version.ToChangeVectorList()?.Select(e => $"{e.NodeTag}:{e.Etag}-{e.DbId}").ToList() ?? []);
            var actual = Describe(() => ParseUtf8(utf8));
            Assert.True(expected == actual, $"'{version}': {expected} vs {actual}");
            return actual;
        }

        private static List<string> ParseUtf8(byte[] utf8)
        {
            var entries = new ParsedChangeVectorEntry[ChangeVectorParser.MaxUtf8Entries(utf8.Length)];
            var count = ChangeVectorParser.ParseUtf8(utf8, entries);
            Assert.True(count >= 0, $"more entries than {entries.Length}");
            return entries.Take(count).Select(e => $"{e.NodeTag}:{e.Etag}-{DbIdOf(e)}").ToList();
        }

        private static string DbIdOf(ParsedChangeVectorEntry entry)
        {
            ReadOnlySpan<char> dbId = entry.DbId;
            return dbId.ToString();
        }

        private static string Describe(Func<List<string>> parse)
        {
            try
            {
                return string.Join(", ", parse());
            }
            catch (Exception e)
            {
                return e.GetType().FullName;
            }
        }

        private static string Compute(string version, bool strict)
        {
            try
            {
                using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
                using (Slice.From(allocator, version, out Slice raw))
                using (RevisionsStorage.GetRevisionKeyHashSlice(allocator, raw, out Slice prefixedHash, strict))
                {
                    return prefixedHash.Size == 0 ? "<empty>" : prefixedHash.ToString();
                }
            }
            catch (Exception e)
            {
                return e.GetType().FullName;
            }
        }

        private static string FortyEntries()
        {
            string[] dbIds = [DbId1, DbId2, DbId3];
            var sb = new StringBuilder();
            for (int i = 0; i < 40; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append("A:").Append(i * 1000).Append('-').Append(dbIds[i % dbIds.Length]);
            }

            return sb.ToString();
        }

        private static string RandomDbId(Random random)
        {
            const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            var chars = new char[22];
            for (int i = 0; i < chars.Length; i++)
                chars[i] = alphabet[random.Next(alphabet.Length)];
            return new string(chars);
        }
    }
}

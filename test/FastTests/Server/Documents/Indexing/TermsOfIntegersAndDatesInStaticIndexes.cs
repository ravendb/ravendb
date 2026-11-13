using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow;
using Sparrow.Extensions;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class TermsOfIntegersAndDatesInStaticIndexes : RavenTestBase
    {
        public TermsOfIntegersAndDatesInStaticIndexes(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public long Long { get; set; }
            public ulong ULong { get; set; }
            public int Int { get; set; }
            public uint UInt { get; set; }
            public short Short { get; set; }
            public ushort UShort { get; set; }
            public byte Byte { get; set; }
            public sbyte SByte { get; set; }
            public DateTime At { get; set; }
            public DateTimeOffset AtOffset { get; set; }
        }

        // the casts make the map produce values of these exact CLR types
        private const string Map = @"
from i in docs.Items
select new
{
    Long = (long)i.Long,
    ULong = (ulong)i.ULong,
    Int = (int)i.Int,
    UInt = (uint)i.UInt,
    Short = (short)i.Short,
    UShort = (ushort)i.UShort,
    Byte = (byte)i.Byte,
    SByte = (sbyte)i.SByte,
    At = i.At,
    AtOffset = i.AtOffset,
    Day = DateOnly.FromDateTime(i.At),
    Time = TimeOnly.FromDateTime(i.At)
}";

        // values on both sides of the cached strings of small numbers (0-299), negatives and the bounds of every type
        private static readonly long[] Longs = { -129, -1, 0, 7, 299, 300, long.MinValue, long.MaxValue };
        private static readonly ulong[] ULongs = { 0, 7, 300, ulong.MaxValue };
        private static readonly int[] Ints = { -129, 0, 300, int.MinValue, int.MaxValue };
        private static readonly uint[] UInts = { 0, 300, uint.MaxValue };
        private static readonly short[] Shorts = { -129, 0, 300, short.MinValue, short.MaxValue };
        private static readonly ushort[] UShorts = { 0, 300, ushort.MaxValue };
        private static readonly byte[] Bytes = { 0, 7, byte.MaxValue };
        private static readonly sbyte[] SBytes = { sbyte.MinValue, -1, sbyte.MaxValue };

        private static readonly DateTime[] Dates =
        {
            new DateTime(2020, 1, 2, 3, 4, 5, 6, DateTimeKind.Utc),
            new DateTime(1999, 12, 31, 23, 59, 59, 999, DateTimeKind.Unspecified).AddTicks(9999),
            new DateTime(2024, 2, 29, 0, 0, 0, DateTimeKind.Utc)
        };

        private static readonly TimeSpan Offset = TimeSpan.FromHours(2);

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void TextualTermsOfIntegersAndDatesProducedByMap(Options options)
        {
            using var store = GetDocumentStore(options);

            // Lucene cannot index ulong values above long.MaxValue (BlittableNumber.Parse throws OverflowException)
            var uLongs = options.SearchEngineMode == RavenSearchEngineMode.Lucene
                ? ULongs.Where(x => x <= long.MaxValue).ToArray()
                : ULongs;

            const int numberOfItems = 8;

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < numberOfItems; i++)
                {
                    var at = Dates[i % Dates.Length];
                    session.Store(new Item
                    {
                        Long = Longs[i % Longs.Length],
                        ULong = uLongs[i % uLongs.Length],
                        Int = Ints[i % Ints.Length],
                        UInt = UInts[i % UInts.Length],
                        Short = Shorts[i % Shorts.Length],
                        UShort = UShorts[i % UShorts.Length],
                        Byte = Bytes[i % Bytes.Length],
                        SByte = SBytes[i % SBytes.Length],
                        At = at,
                        AtOffset = new DateTimeOffset(DateTime.SpecifyKind(at, DateTimeKind.Unspecified), Offset)
                    });
                }

                session.SaveChanges();
            }

            const string indexName = "Items/ByTypedValues";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Name = indexName, Maps = { Map } }));
            Indexes.WaitForIndexing(store);

            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName }));
            Assert.Empty(errors.SelectMany(x => x.Errors));

            AssertTerms("Long", Longs.Select(x => x.ToString(CultureInfo.InvariantCulture)));
            AssertTerms("ULong", uLongs.Select(x => x.ToString(CultureInfo.InvariantCulture)));
            AssertTerms("Int", Ints.Select(x => x.ToString(CultureInfo.InvariantCulture)));
            AssertTerms("UInt", UInts.Select(x => x.ToString(CultureInfo.InvariantCulture)));
            AssertTerms("Short", Shorts.Select(x => x.ToString(CultureInfo.InvariantCulture)));
            AssertTerms("UShort", UShorts.Select(x => x.ToString(CultureInfo.InvariantCulture)));
            AssertTerms("Byte", Bytes.Select(x => x.ToString(CultureInfo.InvariantCulture)));
            AssertTerms("SByte", SBytes.Select(x => x.ToString(CultureInfo.InvariantCulture)));

            AssertTerms("At", Dates.Select(x => x.GetDefaultRavenFormat()));
            AssertTerms("AtOffset", Dates.Select(x => new DateTimeOffset(DateTime.SpecifyKind(x, DateTimeKind.Unspecified), Offset).UtcDateTime.GetDefaultRavenFormat(isUtc: true)));
            AssertTerms("Day", Dates.Select(x => DateOnly.FromDateTime(x).ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture)));
            AssertTerms("Time", Dates.Select(x => TimeOnly.FromDateTime(x).ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture)));

            using (var session = store.OpenSession())
            {
                Assert.Equal(1, session.Advanced.RawQuery<object>($"from index '{indexName}' where Long = -129").Count());
                Assert.Equal(1, session.Advanced.RawQuery<object>($"from index '{indexName}' where Long = {long.MaxValue}").Count());
                Assert.Equal(3, session.Advanced.RawQuery<object>($"from index '{indexName}' where Long between 7 and 300").Count());
                Assert.Equal(6, session.Advanced.RawQuery<object>($"from index '{indexName}' where SByte < 0").Count()); // -128, -1 repeated in 8 items
                Assert.Equal(3, session.Advanced.RawQuery<object>($"from index '{indexName}' where At = $at").AddParameter("at", Dates[0]).Count());
            }

            void AssertTerms(string field, IEnumerable<string> expected)
            {
                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, field, fromValue: null, pageSize: 1024));
                Assert.Equal(expected.Distinct().OrderBy(x => x, StringComparer.Ordinal), terms.OrderBy(x => x, StringComparer.Ordinal));
            }
        }
    }
}

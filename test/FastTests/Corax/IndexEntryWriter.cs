using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using FastTests.Voron;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class IndexEntryWriterTest : StorageTest
    {
        public IndexEntryWriterTest(ITestOutputHelper output) : base(output)
        {
        }

        internal readonly struct StringArrayIterator : IReadOnlySpanIndexer
        {
            private readonly string[] _values;

            public StringArrayIterator(string[] values)
            {
                _values = values;
            }

            public int Length => _values.Length;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return _values[i] == null;
            }

            public ReadOnlySpan<byte> this[int i] => _values[i] == null ? ReadOnlySpan<byte>.Empty : Encoding.UTF8.GetBytes(_values[i]);
        }

        [Fact]
        public void SimpleWrites()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            using var _ = StorageEnvironment.GetStaticContext(out var ctx);
            Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
            Slice.From(ctx, "B", ByteStringType.Immutable, out Slice bSlice);
            Slice.From(ctx, "C", ByteStringType.Immutable, out Slice cSlice);
            Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

            // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
            var knownFields = new IndexFieldsMapping(ctx)
                                    .AddBinding(0, aSlice)
                                    .AddBinding(1, bSlice)
                                    .AddBinding(2, cSlice)
                                    .AddBinding(3, dSlice);

            var writer = new IndexEntryWriter(bsc, knownFields);
            writer.Write(0, Encoding.UTF8.GetBytes("1.001"), 1, 1.001);
            writer.Write(1, new StringArrayIterator(new[] { "AAA", "BF", "CE" }));
            writer.Write(2, Encoding.UTF8.GetBytes("CCCC"));
            writer.Write(3, Encoding.UTF8.GetBytes("DDDDDDDDDD"));
            using var __ = writer.Finish(out var element);

            var reader = new IndexEntryReader(element.ToSpan());
            reader.Read(0, out long longValue);
            Assert.Equal(1, longValue);
            reader.Read(0, out int intValue);
            Assert.Equal(1, intValue);
            
            Assert.True(reader.GetFieldType(0, out var _).HasFlag(IndexEntryFieldType.Tuple));
            Assert.False(reader.GetFieldType(0, out var _).HasFlag(IndexEntryFieldType.List));
            Assert.True(reader.GetFieldType(1, out var _).HasFlag(IndexEntryFieldType.List));
            Assert.False(reader.GetFieldType(1, out var _).HasFlag(IndexEntryFieldType.Tuple));
            Assert.True(reader.GetFieldType(2, out var _).HasFlag(IndexEntryFieldType.Simple));
            Assert.True(reader.GetFieldType(3, out var _).HasFlag(IndexEntryFieldType.Simple));

            reader.Read(0, out double doubleValue);
            Assert.True(doubleValue.AlmostEquals(1.001));
            reader.Read(0, out double floatValue);
            Assert.True(floatValue.AlmostEquals(1.001));

            reader.TryReadTuple(0, out longValue, out doubleValue, out var sequenceValue);
            Assert.True(doubleValue.AlmostEquals(1.001));
            Assert.Equal(1, longValue);
            Assert.True(sequenceValue.SequenceCompareTo(Encoding.UTF8.GetBytes("1.001").AsSpan()) == 0);

            reader.Read(2, value: out sequenceValue);
            Assert.True(sequenceValue.SequenceCompareTo(Encoding.UTF8.GetBytes("CCCC").AsSpan()) == 0);
            reader.Read(3, value: out sequenceValue);
            Assert.True(sequenceValue.SequenceCompareTo(Encoding.UTF8.GetBytes("DDDDDDDDDD").AsSpan()) == 0);

            reader.Read(1, value: out sequenceValue, elementIdx: 0);
            Assert.True(sequenceValue.SequenceCompareTo(Encoding.UTF8.GetBytes("AAA").AsSpan()) == 0);
            reader.Read(1, value: out sequenceValue, elementIdx: 2);
            Assert.True(sequenceValue.SequenceCompareTo(Encoding.UTF8.GetBytes("CE").AsSpan()) == 0);
        }

        

        [Fact]
        public void IterationReads()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            using var _ = StorageEnvironment.GetStaticContext(out var ctx);
            Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
            Slice.From(ctx, "B", ByteStringType.Immutable, out Slice bSlice);
            Slice.From(ctx, "C", ByteStringType.Immutable, out Slice cSlice);
            Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

            // The idea is that GetField will return an struct we can use later on a loop (we just get it once).

            var knownFields = new IndexFieldsMapping(ctx)
                                    .AddBinding(0, aSlice)
                                    .AddBinding(1, bSlice)
                                    .AddBinding(2, cSlice)
                                    .AddBinding(3, dSlice);

            string[] values =
            {
                "A",
                "BB",
                "CCC",
                "DDDD",
                "EEEEE"
            };

            Span<long> longValues = new long[] { 1, 2, 3, 4, 5 };
            Span<double> doubleValues = new[] { 1.01, 2.01, 3.01, 4.01, 5.01 };

            var writer = new IndexEntryWriter(bsc, knownFields);
            writer.Write(0, new StringArrayIterator(values));
            writer.Write(1, new StringArrayIterator(values), longValues, doubleValues);
            writer.Write(2, Encoding.UTF8.GetBytes(values[3]));
            using var ___ = writer.Finish(out var element);

            var reader = new IndexEntryReader(element.ToSpan());

            // Get the first
           Assert.True(reader.TryReadMany(1, out var fieldIterator));
            Assert.True(fieldIterator.ReadNext());
            Assert.True(fieldIterator.Double.AlmostEquals(1.01));
            Assert.Equal(1, fieldIterator.Long);
            Assert.True(fieldIterator.Sequence.SequenceCompareTo(Encoding.UTF8.GetBytes(values[0]).AsSpan()) == 0);

            Assert.False(reader.TryReadMany(2, out  fieldIterator));

            fieldIterator = reader.ReadMany(1);
            Assert.Equal(5, fieldIterator.Count);

            int i = 0;
            while (fieldIterator.ReadNext())
            {
                Assert.True(fieldIterator.Double.AlmostEquals(i + 1.01));
                Assert.Equal(1 + i, fieldIterator.Long);
                Assert.True(fieldIterator.Sequence.SequenceCompareTo(Encoding.UTF8.GetBytes(values[i]).AsSpan()) == 0);
                i++;
            }

            try { var __ = fieldIterator.Double; } catch (IndexOutOfRangeException) {}
            try { var __ = fieldIterator.Long; } catch (IndexOutOfRangeException) {}
            try { var __ = fieldIterator.Sequence; } catch (IndexOutOfRangeException) { }

            fieldIterator = reader.ReadMany(0);
            Assert.Equal(5, fieldIterator.Count);

            i = 0;
            while (fieldIterator.ReadNext())
            {
                try { var __ = fieldIterator.Double; } catch (InvalidOperationException) { }
                try { var __ = fieldIterator.Long; } catch (InvalidOperationException) { }
                Assert.True(fieldIterator.Sequence.SequenceCompareTo(Encoding.UTF8.GetBytes(values[i]).AsSpan()) == 0);
                i++;
            }
        }

        [Fact]
        public void SimpleWriteNulls()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            using var _ = StorageEnvironment.GetStaticContext(out var ctx);
            Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
            Slice.From(ctx, "B", ByteStringType.Immutable, out Slice bSlice);
            Slice.From(ctx, "C", ByteStringType.Immutable, out Slice cSlice);
            Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

            // The idea is that GetField will return an struct we can use later on a loop (we just get it once).

            var knownFields = new IndexFieldsMapping(ctx)
                                    .AddBinding(0, aSlice)
                                    .AddBinding(1, bSlice)
                                    .AddBinding(2, cSlice)
                                    .AddBinding(3, dSlice);

            string[] values =
            {
                "A",
                null,
                "CCC",
            };

            Span<long> longValues = new long[] { 1, 2, 3 };
            Span<double> doubleValues = new[] { 1.01, 2.01, 3.01 };

            var writer = new IndexEntryWriter(bsc, knownFields);
            writer.Write(0, new StringArrayIterator(values));
            writer.Write(1, new StringArrayIterator(values), longValues, doubleValues);
            using var ___ = writer.Finish(out var element);

            var reader = new IndexEntryReader(element.ToSpan());                        
            
            // Get the first
            Assert.True(reader.TryReadMany(1, out var fieldIterator));
            Assert.True(fieldIterator.ReadNext());
            Assert.True(fieldIterator.Double.AlmostEquals(1.01));
            Assert.Equal(1, fieldIterator.Long);
            Assert.True(fieldIterator.Sequence.SequenceCompareTo(Encoding.UTF8.GetBytes(values[0]).AsSpan()) == 0);

            Assert.False(reader.Read(2, out var _));
            Assert.False(reader.TryReadMany(2, out fieldIterator));

            fieldIterator = reader.ReadMany(1);
            Assert.Equal(3, fieldIterator.Count);

            int i = 0;
            while (fieldIterator.ReadNext())
            {
                if (values[i] == null)
                {
                    Assert.True(fieldIterator.IsNull);
                    try
                    { var __ = fieldIterator.Sequence; }
                    catch (NullReferenceException) { }
                    try
                    { var __ = fieldIterator.Double; }
                    catch (NullReferenceException) { }
                    try
                    { var __ = fieldIterator.Long; }
                    catch (NullReferenceException) { }
                }
                else
                {
                    Assert.False(fieldIterator.IsNull);
                    Assert.True(fieldIterator.Double.AlmostEquals(i + 1.01));
                    Assert.Equal(1 + i, fieldIterator.Long);
                    Assert.True(fieldIterator.Sequence.SequenceCompareTo(Encoding.UTF8.GetBytes(values[i]).AsSpan()) == 0);
                }

                i++;
            }

            fieldIterator = reader.ReadMany(0);
            Assert.Equal(3, fieldIterator.Count);

            i = 0;
            while (fieldIterator.ReadNext())
            {
                if (values[i] == null)
                {
                    Assert.True(fieldIterator.IsNull);
                    try
                    { var __ = fieldIterator.Sequence; }
                    catch (NullReferenceException) { }
                    try
                    { var __ = fieldIterator.Double; }
                    catch (InvalidOperationException) { }
                    try
                    { var __ = fieldIterator.Long; }
                    catch (InvalidOperationException) { }
                }
                else
                {
                    Assert.False(fieldIterator.IsNull);
                    try
                    { var __ = fieldIterator.Double; }
                    catch (InvalidOperationException) { }
                    try
                    { var __ = fieldIterator.Long; }
                    catch (InvalidOperationException) { }
                    Assert.True(fieldIterator.Sequence.SequenceCompareTo(Encoding.UTF8.GetBytes(values[i]).AsSpan()) == 0);
                }
                                
                i++;
            }
        }

        [Fact]
        public void SimpleWriteEmpty()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            using var _ = StorageEnvironment.GetStaticContext(out var ctx);
            Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
            Slice.From(ctx, "B", ByteStringType.Immutable, out Slice bSlice);
            Slice.From(ctx, "C", ByteStringType.Immutable, out Slice cSlice);
            Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

            // The idea is that GetField will return an struct we can use later on a loop (we just get it once).

            var knownFields = new IndexFieldsMapping(ctx)
                                    .AddBinding(0, aSlice)
                                    .AddBinding(1, bSlice)
                                    .AddBinding(2, cSlice)
                                    .AddBinding(3, dSlice);

            string[] values = { };
            Span<long> longValues = new long[] { };
            Span<double> doubleValues = new double[] { };

            var writer = new IndexEntryWriter(bsc, knownFields);
            writer.Write(0, new StringArrayIterator(values));
            writer.Write(1, new StringArrayIterator(values), longValues, doubleValues);
            using var ___ = writer.Finish(out var element);

            var reader = new IndexEntryReader(element.ToSpan());
            Assert.True(reader.Read(1, out var type, out var longValue, out var doubleValue, out var sequenceValue));
            Assert.True(type.HasFlag(IndexEntryFieldType.Empty));
            Assert.True(type.HasFlag(IndexEntryFieldType.List));
            Assert.Equal(0, sequenceValue.Length);

            Assert.True(reader.Read(1, out type, out sequenceValue));
            Assert.True(type.HasFlag(IndexEntryFieldType.Empty));
            Assert.True(type.HasFlag(IndexEntryFieldType.List));
            Assert.Equal(0, sequenceValue.Length);

            reader = new IndexEntryReader(element.ToSpan());
            Assert.True(reader.TryReadMany(1, out var iterator));
            type = reader.GetFieldType(1, out var offset);
            Assert.True(type.HasFlag(IndexEntryFieldType.Empty));
            Assert.True(type.HasFlag(IndexEntryFieldType.List));
            Assert.False(iterator.ReadNext());
            Assert.Equal(0, iterator.Count);
            
            Assert.False(reader.TryReadMany(2, out var fieldIterator));

            fieldIterator = reader.ReadMany(1);
            Assert.Equal(0, fieldIterator.Count);
            Assert.True(fieldIterator.IsEmpty);

            try
            { var __ = fieldIterator.IsNull; }
            catch (InvalidOperationException) { }
            try
            { var __ = fieldIterator.Sequence; }
            catch (IndexOutOfRangeException) { }
            try
            { var __ = fieldIterator.Double; }
            catch (IndexOutOfRangeException) { }
            try
            { var __ = fieldIterator.Long; }
            catch (IndexOutOfRangeException) { }

            fieldIterator = reader.ReadMany(0);
            Assert.Equal(0, fieldIterator.Count);
            Assert.True(fieldIterator.IsEmpty);

            try
            { var __ = fieldIterator.IsNull; }
            catch (InvalidOperationException) { }
            try
            { var __ = fieldIterator.Sequence; }
            catch (IndexOutOfRangeException) { }
            try
            { var __ = fieldIterator.Double; }
            catch (InvalidOperationException) { }
            try
            { var __ = fieldIterator.Long; }
            catch (InvalidOperationException) { }            
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(1337)]
        public void WriteMultipleLongLists(int seed)
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            using var _ = StorageEnvironment.GetStaticContext(out var ctx);
            Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
            Slice.From(ctx, "B", ByteStringType.Immutable, out Slice bSlice);
            Slice.From(ctx, "C", ByteStringType.Immutable, out Slice cSlice);
            Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

            // The idea is that GetField will return an struct we can use later on a loop (we just get it once).

            var knownFields = new IndexFieldsMapping(ctx)
                                    .AddBinding(0, aSlice)
                                    .AddBinding(1, bSlice)
                                    .AddBinding(2, cSlice)
                                    .AddBinding(3, dSlice);
            var random = new Random(seed);
            
            string RandomString(int length)
            {
                const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
                var str = new char[length];
                for (int i = 0; i < length; i++)
                {
                    str[i] = chars[random.Next(chars.Length)];
                }
                return new string(str);
            }

            string[] values = new string[64];
            long[] longs = new long[64];
            double[] doubles = new double[64];
            for (int ii = 0; ii < values.Length; ii++)
            {
                if (random.Next(0, 10) == 0)
                    continue;

                if (random.Next(0, 10) == 0)
                    values[ii] = String.Empty;
                else
                    values[ii] = $"{ii}-{RandomString(random.Next(64))}";
                                    
                longs[ii] = ii;
                doubles[ii] = ii + (ii / 65.0);
            }

            var writer = new IndexEntryWriter(bsc, knownFields);
            writer.Write(2, new StringArrayIterator(values), longs, doubles);
            writer.Write(0, new StringArrayIterator(values));
            writer.Write(1, new StringArrayIterator(new string[0]));
            writer.Write(3, new StringArrayIterator(values), longs, doubles);
            using var ___ = writer.Finish(out var element);

            var reader = new IndexEntryReader(element.ToSpan());

            var iterator = reader.ReadMany(0);
            Assert.True(iterator.IsValid);
            Assert.False(iterator.IsEmpty);
            int i = 0;
            while (iterator.ReadNext())
            {
                if (values[i] == null)
                {
                    Assert.True(iterator.IsNull);
                }
                else
                {
                    Assert.False(iterator.IsNull);
                    Assert.False(iterator.IsEmpty);
                    Assert.Equal(values[i], Encoding.UTF8.GetString(iterator.Sequence));
                }
                i++;
            }

            Assert.Equal(64, i);

            iterator = reader.ReadMany(1);
            Assert.True(iterator.IsValid);
            Assert.True(iterator.IsEmpty);

            iterator = reader.ReadMany(2);
            Assert.True(iterator.IsValid);
            Assert.False(iterator.IsEmpty);

            i = 0;
            while (iterator.ReadNext())
            {
                if (values[i] == null)
                {
                    Assert.True(iterator.IsNull);
                }
                else
                {
                    Assert.False(iterator.IsNull);
                    Assert.False(iterator.IsEmpty);
                    Assert.Equal(values[i], Encoding.UTF8.GetString(iterator.Sequence));
                    Assert.Equal(i, iterator.Long);
                    Assert.True((i + (i / 65.0) - iterator.Double) < 0.0001);
                }
                i++;
            }

            iterator = reader.ReadMany(3);
            Assert.True(iterator.IsValid);
            Assert.False(iterator.IsEmpty);

            i = 0;
            while (iterator.ReadNext())
            {
                if (values[i] == null)
                {
                    Assert.True(iterator.IsNull);
                }
                else
                {
                    Assert.False(iterator.IsNull);
                    Assert.False(iterator.IsEmpty);
                    Assert.Equal(values[i], Encoding.UTF8.GetString(iterator.Sequence));
                    Assert.Equal(i, iterator.Long);
                    Assert.True((i + (i / 65.0) - iterator.Double) < 0.0001);
                }
                i++;
            }
        }
    }
}

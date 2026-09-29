using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace SlowTests.Issues;

public class RavenDB_27458_LowLevel : StorageTest
{
    private const int IdIndex = 0, TextIndex = 1, NumberIndex = 2;

    private const string TextValue = "x";

    // Number is 0..9 for the first entries and null for the rest.
    private const int EntriesWithValue = 10, EntriesWithNull = 5;

    private IndexFieldsMapping _knownFields;

    public RavenDB_27458_LowLevel(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(UnaryMatchOperation.LessThan, 3L)]
    [InlineData(UnaryMatchOperation.LessThanOrEqual, 3L)]
    [InlineData(UnaryMatchOperation.GreaterThan, 7L)]
    [InlineData(UnaryMatchOperation.GreaterThanOrEqual, 7L)]
    public void MultiUnaryMatchMustAgreeWithRangeQueryOnNull(UnaryMatchOperation operation, long value)
    {
        IndexEntries();

        using var searcher = new IndexSearcher(Env, _knownFields);

        var numberField = FieldMetadataFor(NumberIndex);
        var textField = FieldMetadataFor(TextIndex);

        // The same predicate in its two representations: the range query walks the field's term tree and never sees
        // the null posting list, while MultiUnaryMatch scans the entries of another clause and compares in place.
        // Both must agree, and neither may accept an entry whose Number is null.
        var rangeQuery = RangeQueryFor(searcher, numberField, operation, value);
        var multiUnaryMatch = searcher.CreateMultiUnaryMatch(searcher.TermQuery(textField, TextValue),
            [new MultiUnaryItem(numberField, value, operation)]);

        var expected = Expected(operation, value);

        Assert.Equal(expected, Fetch(searcher, ref rangeQuery));
        Assert.Equal(expected, Fetch(searcher, ref multiUnaryMatch));
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void NullEntriesAreIndexedAsNull()
    {
        // Guards the theory above: if the null entries were not indexed as null at all, no comparison could accept
        // them and the theory would pass for the wrong reason.
        IndexEntries();

        using var searcher = new IndexSearcher(Env, _knownFields);

        Assert.True(searcher.TryGetPostingListForNull(FieldMetadataFor(NumberIndex), out long nullPostingListId));
        Assert.Equal(EntriesWithNull, searcher.GetPostingList(nullPostingListId).State.NumberOfEntries);
    }

    private static List<string> Expected(UnaryMatchOperation operation, long value)
    {
        var ids = new List<string>();

        // deliberately over the entries that have a value only - a null belongs to no range
        for (int i = 0; i < EntriesWithValue; i++)
        {
            var matches = operation switch
            {
                UnaryMatchOperation.LessThan => i < value,
                UnaryMatchOperation.LessThanOrEqual => i <= value,
                UnaryMatchOperation.GreaterThan => i > value,
                UnaryMatchOperation.GreaterThanOrEqual => i >= value,
                _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null)
            };

            if (matches)
                ids.Add(IdOf(i));
        }

        return ids;
    }

    private static MultiTermMatch RangeQueryFor(IndexSearcher searcher, in FieldMetadata field, UnaryMatchOperation operation, long value)
    {
        return operation switch
        {
            UnaryMatchOperation.LessThan => searcher.LessThanQuery<long>(field, value),
            UnaryMatchOperation.LessThanOrEqual => searcher.LessThanOrEqualsQuery<long>(field, value),
            UnaryMatchOperation.GreaterThan => searcher.GreaterThanQuery<long>(field, value),
            UnaryMatchOperation.GreaterThanOrEqual => searcher.GreatThanOrEqualsQuery<long>(field, value),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null)
        };
    }

    private static List<string> Fetch<TMatch>(IndexSearcher searcher, ref TMatch match)
        where TMatch : IQueryMatch
    {
        var reader = searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName());
        var ids = new List<string>();

        Span<long> buffer = stackalloc long[64];
        int read;
        while ((read = match.Fill(buffer)) > 0)
        {
            for (int i = 0; i < read; i++)
                ids.Add(reader.GetTermFor(buffer[i]));
        }

        ids.Sort(StringComparer.Ordinal);
        return ids;
    }

    private FieldMetadata FieldMetadataFor(int fieldId)
    {
        return FieldMetadata.Build(_knownFields.GetByFieldId(fieldId).FieldName, default, fieldId, default, default);
    }

    private static string IdOf(int index) => $"items/{index:00}";

    private void IndexEntries()
    {
        _knownFields = CreateKnownFields(Allocator);

        using var indexWriter = new IndexWriter(Env, _knownFields, SupportedFeatures.All);

        for (int i = 0; i < EntriesWithValue + EntriesWithNull; i++)
        {
            var id = IdOf(i);

            using var builder = indexWriter.Index(id);
            builder.Write(IdIndex, Encoding.UTF8.GetBytes(id));
            builder.Write(TextIndex, Encoding.UTF8.GetBytes(TextValue));

            if (i < EntriesWithValue)
                builder.Write(NumberIndex, Encoding.UTF8.GetBytes(i.ToString(CultureInfo.InvariantCulture)), i, i);
            else
                builder.WriteNull(NumberIndex, path: null);

            builder.EndWriting();
        }

        indexWriter.Commit();
    }

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Text", ByteStringType.Immutable, out Slice textSlice);
        Slice.From(ctx, "Number", ByteStringType.Immutable, out Slice numberSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdIndex, idSlice)
            .AddBinding(TextIndex, textSlice)
            .AddBinding(NumberIndex, numberSlice);

        return builder.Build();
    }
}

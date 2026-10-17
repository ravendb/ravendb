using System;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private sealed class DocWithArray
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
        public string[] Categories { get; set; }
        public int[] Numbers { get; set; }
    }

    private sealed class TagCount
    {
        public string Tag { get; set; }
        public int Count { get; set; }
    }

    private sealed class CategoryCount
    {
        public string Category { get; set; }
        public int Count { get; set; }
    }

    private sealed class DocWithLongs
    {
        public string Id { get; set; }
        public long[] Values { get; set; }
        public int[] IntValues { get; set; }
        public ulong[] ULongValues { get; set; }
    }

    private sealed class DocWithStrings
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
    }

    private sealed class DocWithDates
    {
        public string Id { get; set; }
        public DateTime[] ImportantDates { get; set; }
    }

    private sealed class DocWithDoubles
    {
        public string Id { get; set; }
        public double[] Values { get; set; }
    }

    private sealed class DocWithFloats
    {
        public string Id { get; set; }
        public float[] Values { get; set; }
    }

    private sealed class DocWithDecimals
    {
        public string Id { get; set; }
        public decimal[] Values { get; set; }
    }

    private sealed class DocWithChars
    {
        public string Id { get; set; }
        public char[] Values { get; set; }
    }

    private sealed class DocWithBools
    {
        public string Id { get; set; }
        public bool[] Values { get; set; }
    }

    private sealed class ItemWithTags
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
    }

    private sealed class DocWithNestedArray
    {
        public string Id { get; set; }
        public ItemWithTags[] Items { get; set; }
    }
}

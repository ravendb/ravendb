using Sparrow;

namespace Raven.Server.Documents.Queries.Sorting
{
    public static class SortFieldHelper // TODO arek - remove me
    {
        private static readonly char[] Separators = { ';' };

        public static StringSegment ExtractName(StringSegment field)
        {
            var indexOfSeparator = field.IndexOfAny(Separators, 0);
            if (indexOfSeparator == -1)
                return field;

            return field.SubSegment(indexOfSeparator + 1);
        }
    }
}
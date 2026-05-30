using System;

namespace Sparrow.Utils
{
    internal static class ChangeVectorStringReader
    {
        public static long GetOrderEtagById(string changeVector, string id)
        {
            var separatorIndex = ChangeVectorParts.GetCompositeSeparatorIndex(changeVector);
            return separatorIndex < 0
                ? GetEtagByIdInternal(changeVector, id, startIndex: 0, count: changeVector?.Length ?? 0)
                : GetEtagByIdInternal(changeVector, id, startIndex: 0, count: separatorIndex);
        }

        public static long GetVersionEtagById(string changeVector, string id)
        {
            var separatorIndex = ChangeVectorParts.GetCompositeSeparatorIndex(changeVector);
            return separatorIndex < 0
                ? GetEtagByIdInternal(changeVector, id, startIndex: 0, count: changeVector?.Length ?? 0)
                : GetEtagByIdInternal(changeVector, id, startIndex: separatorIndex + 1, count: changeVector.Length - separatorIndex - 1);
        }

        public static string GetOrderNodeTagById(string changeVector, string id)
        {
            var separatorIndex = ChangeVectorParts.GetCompositeSeparatorIndex(changeVector);
            return separatorIndex < 0
                ? GetNodeTagByIdInternal(changeVector, id, startIndex: 0, count: changeVector?.Length ?? 0)
                : GetNodeTagByIdInternal(changeVector, id, startIndex: 0, count: separatorIndex);
        }

        public static string GetVersionNodeTagById(string changeVector, string id)
        {
            var separatorIndex = ChangeVectorParts.GetCompositeSeparatorIndex(changeVector);
            return separatorIndex < 0
                ? GetNodeTagByIdInternal(changeVector, id, startIndex: 0, count: changeVector?.Length ?? 0)
                : GetNodeTagByIdInternal(changeVector, id, startIndex: separatorIndex + 1, count: changeVector.Length - separatorIndex - 1);
        }

        private static long GetEtagByIdInternal(string changeVector, string id, int startIndex, int count)
        {
            if (changeVector == null)
                return 0;

            if (id == null)
                throw new ArgumentNullException(nameof(id));

            var index = IndexOfId(changeVector, id, startIndex, count);
            if (index == -1)
                return 0;

            int end = index - 1;
            int separator = changeVector.LastIndexOf(':', startIndex: end, count: end - startIndex + 1);
            if (separator < startIndex)
                return 0;

            int start = separator + 1;

            return ParseToLong(changeVector, start, length: end - start + 1);
        }

        private static string GetNodeTagByIdInternal(string changeVector, string id, int startIndex, int count)
        {
            if (changeVector == null)
                return null;

            if (id == null)
                throw new ArgumentNullException(nameof(id));

            int indexOfId = IndexOfId(changeVector, id, startIndex, count);
            if (indexOfId <= startIndex)
                return null;

            int endOfNodeTag = changeVector.LastIndexOf(":", startIndex: indexOfId - 1, count: indexOfId - startIndex, StringComparison.Ordinal);
            if (endOfNodeTag < startIndex)
                return null;

            int separator = changeVector.LastIndexOf(", ", startIndex: endOfNodeTag - 1, count: endOfNodeTag - startIndex, StringComparison.OrdinalIgnoreCase);
            int start = separator >= 0 ? separator + 2 : startIndex;

            return changeVector.Substring(start, length: endOfNodeTag - start);
        }

        private static long ParseToLong(string s, int start, int length)
        {
            var num = (long)(s[start] - '0');
            for (var i = 1; i < length; i++)
            {
                num *= 10;
                num += s[start + i] - '0';
            }

            return num;
        }

        private static int IndexOfId(string changeVector, string id, int startIndex, int count)
        {
            if (id.Length == 0)
                return -1;

            var endIndex = startIndex + count;
            for (var searchIndex = startIndex; searchIndex < endIndex;)
            {
                int remaining = endIndex - searchIndex;
                var idIndex = changeVector.IndexOf(id, searchIndex, count: remaining, StringComparison.Ordinal);
                if (idIndex < 0)
                    return -1;

                if (idIndex > startIndex && changeVector[idIndex - 1] == '-')
                    return idIndex - 1;

                searchIndex = idIndex + id.Length;
            }

            return -1;
        }
    }
}

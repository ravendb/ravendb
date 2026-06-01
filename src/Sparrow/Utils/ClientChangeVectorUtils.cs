using System;

namespace Sparrow.Utils
{
    internal static class ClientChangeVectorUtils
    {
        internal const string Separator = "|";

        public static long GetEtagById(string changeVector, string id)
        {
            if (changeVector == null)
                return 0;

            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (changeVector.Contains(Separator))
                throw new ArgumentException($"Change vector contains '{Separator}', which is not supported for this operation.", nameof(changeVector));

            var index = changeVector.IndexOf("-" + id, StringComparison.Ordinal);
            if (index == -1)
                return 0;

            var end = index - 1;
            var start = changeVector.LastIndexOf(":", end, StringComparison.Ordinal) + 1;

            return long.Parse(changeVector.Substring(start, end - start + 1));
        }
    }
}

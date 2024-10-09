using System;

namespace Sparrow.Utils
{
    internal static class ClientChangeVectorUtils
    {
        public static long GetEtagById(string changeVector, string id)
        {
            if (changeVector == null)
                return 0;

            if (id == null)
                throw new ArgumentNullException(nameof(id));

            var index = changeVector.IndexOf("-" + id, StringComparison.Ordinal);
            if (index == -1)
                return 0;

            var end = index - 1;
            var start = changeVector.LastIndexOf(":", end, StringComparison.Ordinal) + 1;

            return long.Parse(changeVector.Substring(start, end - start + 1));
        }
    }
}

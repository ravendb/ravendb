using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace System
{
    public static class EnumerableExtensions
    {
        /// <summary>
        /// This extensions is based on the C# 9.0 capability to force any <![CDATA[ IEnumerator<T> ]]> into a foreach
        /// The trick is to use an extension method that will always match.
        /// https://github.com/dotnet/csharplang/blob/6e748b19f1076cc7109293f1038e6042e4ac7f06/meetings/2020/LDM-2020-03-23.md#allow-getenumerator-from-extension-methods
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IEnumerator<T> GetEnumerator<T>(this IEnumerator<T> enumerator) => enumerator;
    }
}

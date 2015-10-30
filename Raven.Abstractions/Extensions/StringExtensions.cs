using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Extensions
{
    public static class StringExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ConvertToString(this byte[] buffer, int size)
        {
            return Encoding.UTF8.GetString(buffer, 0, size);
        }
    }
}

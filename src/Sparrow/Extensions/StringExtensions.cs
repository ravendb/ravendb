using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace Sparrow.Extensions
{
    public static class StringExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetUtf8MaxSize(this string value)
        {
            int ascii = 1; // We account for the end of the string. 
            int nonAscii = 0;
            for (int i = 0; i < value.Length; i++)
            {
                char v = value[i];
                if (v <= 0x7F)
                    ascii++;
                else if (v <= 0x7FF)
                    ascii += 2;
                else
                    nonAscii++;
            }

            // We can do 4 because unicode (string is unicode encoded) doesnt support 5 and 6 bytes values.
            int result = ascii + nonAscii * 4; 
            Debug.Assert( result >= Encoding.UTF8.GetByteCount(value));

            return result;
        }
    }
}

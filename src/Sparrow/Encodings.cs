using System.Text;

namespace Sparrow
{
    internal static class Encodings
    {
        public static readonly UTF8Encoding Utf8;

        static Encodings()
        {
            Utf8 = new UTF8Encoding();
        }
    }
}

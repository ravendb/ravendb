namespace Sparrow.Global
{
    internal static class Constants
    {
        internal static class Size
        {
            public const int Kilobyte = 1024;
            public const int Megabyte = 1024 * Kilobyte;
            public const int Gigabyte = 1024 * Megabyte;
            public const long Terabyte = 1024 * (long)Gigabyte;
        }

        internal static class Encryption
        {
            public static readonly int XChachaAdLen = (int)Sodium.crypto_secretstream_xchacha20poly1305_abytes();
            public const int DefaultBufferSize = 4096;
        }
    }
}

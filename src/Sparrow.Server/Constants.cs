namespace Sparrow.Server.Global
{
    internal static class Constants
    {
        internal static class Encryption
        {
            public static readonly int XChachaAdLen = (int)Sodium.crypto_secretstream_xchacha20poly1305_abytes();
            public const int DefaultBufferSize = 4096;
        }
    }
}

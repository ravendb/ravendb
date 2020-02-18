using System.Runtime.InteropServices;

namespace Voron.Data.Tables
{
    [StructLayout(LayoutKind.Sequential)]
    public struct CompressionDictionaryInfo
    {
        public byte ExpectedCompressionRatio;
    }
}

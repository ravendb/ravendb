namespace Voron.Data.Compression
{
    public class UncompressedEntry
    {
        public Slice Key;

        public int Length;

        public UncompressedEntry Set(Slice key, int len)
        {
            Key = key;
            Length = len;

            return this;
        }
    }
}
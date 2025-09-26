namespace Voron
{
    /// <summary>
    /// A struct representing a read result.
    /// If the read returns a null, <see cref="IsNull"/> is true. Otherwise, the <see cref="ValueReader"/> can be accessed via <see cref="Reader"/>.
    /// </summary>
    public readonly unsafe struct ReadResult(byte* val, int len)
    {
        public ReadResult(in ValueReader reader) : this(reader.Base, reader.Length) { }

        public ValueReader Reader => new(val, len);

        public bool IsNull => val == null;

        public static readonly ReadResult Null = default;

        // The null handling helper methods.
        
        public T[] ToArrayEmptyOnNull<T>() where T : unmanaged => IsNull ? [] : Reader.ToUnmanagedSpan<T>().ToSpan().ToArray();

        public long ReadLittleEndianInt64OrDefault(long defaultValue = 0) => IsNull ? defaultValue : Reader.ReadLittleEndianInt64();
        
        public int ReadLittleEndianInt32OrDefault(int defaultValue = 0) => IsNull ? defaultValue : Reader.ReadLittleEndianInt32();
        
        public byte ReadByteOrDefault(byte defaultValue = 0) => IsNull ? defaultValue : Reader.ReadByte();

        public string ToStringValueOrDefault(string defaultValue = null) => IsNull ? defaultValue : Reader.ToStringValue();
    }
}

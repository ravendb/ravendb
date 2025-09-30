namespace Voron
{
    /// <summary>
    /// A struct representing a read result.
    /// If the read returns a null, <see cref="HasValue"/> is false. Otherwise, the <see cref="ValueReader"/> can be accessed via <see cref="Reader"/>.
    /// </summary>
    public readonly unsafe struct ReadResult(byte* val, int len)
    {
        public ReadResult(in ValueReader reader) : this(reader.Base, reader.Length) { }

        public ValueReader Reader => new(val, len);

        public bool HasValue => val != null;

        public static readonly ReadResult Null = default;

        // The null handling helper methods.
        
        public T[] ToArrayEmptyOnNull<T>() where T : unmanaged => HasValue ? Reader.ToUnmanagedSpan<T>().ToSpan().ToArray() : [];

        public long ReadLittleEndianInt64OrDefault(long defaultValue = 0) => HasValue ? Reader.ReadLittleEndianInt64() : defaultValue;
        
        public int ReadLittleEndianInt32OrDefault(int defaultValue = 0) => HasValue ? Reader.ReadLittleEndianInt32() : defaultValue;
        
        public byte ReadByteOrDefault(byte defaultValue = 0) => HasValue ? Reader.ReadByte() : defaultValue;

        public string ToStringValueOrDefault(string defaultValue = null) => HasValue ? Reader.ToStringValue() : defaultValue;
    }
}

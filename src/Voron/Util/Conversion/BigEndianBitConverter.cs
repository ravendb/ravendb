
namespace Voron.Util.Conversion
{
    /// <summary>
    /// Implementation of EndianBitConverter which converts to/from big-endian
    /// byte arrays.
    /// </summary>
    public sealed class BigEndianBitConverter : EndianBitConverter
    {
        /// <summary>
        /// Indicates the byte order ("endianess") in which data is converted using this class.
        /// </summary>
        /// <remarks>
        /// Different computer architectures store data using different byte orders. "Big-endian"
        /// means the most significant byte is on the left end of a word. "Little-endian" means the 
        /// most significant byte is on the right end of a word.
        /// </remarks>
        /// <returns>true if this converter is little-endian, false otherwise.</returns>
        public override bool IsLittleEndian()
        {
            return false;
        }

        /// <summary>
        /// Indicates the byte order ("endianess") in which data is converted using this class.
        /// </summary>
        public override Endianness Endianness 
        { 
            get { return Endianness.BigEndian; }
        }

        /// <summary>
        /// Copies the specified number of bytes from value to buffer, starting at index.
        /// </summary>
        /// <param name="value">The value to copy</param>
        /// <param name="bytes">The number of bytes to copy</param>
        /// <param name="buffer">The buffer to copy the bytes into</param>
        /// <param name="index">The index to start at</param>
        protected override void CopyBytesImpl(long value, int bytes, byte[] buffer, int index)
        {
            int endOffset = index+bytes-1;
            for (int i=0; i < bytes; i++)
            {
                buffer[endOffset-i] = unchecked((byte)(value&0xff));
                value = value >> 8;
            }
        }
        
        /// <summary>
        /// Returns a value built from the specified number of bytes from the given buffer,
        /// starting at index.
        /// </summary>
        /// <param name="value">The data in byte array format</param>
        /// <param name="startIndex">The first index to use</param>
        /// <param name="bytesToConvert">The number of bytes to use</param>
        /// <returns>The value built from the given bytes</returns>
        protected override long FromBytes(byte[] value, int startIndex, int bytesToConvert)
        {
            long ret = 0;
            for (int i=0; i < bytesToConvert; i++)
            {
                ret = unchecked((ret << 8) | value[startIndex+i]);
            }
            return ret;
        }

        protected override unsafe long FromBytes(byte* value, int bytesToConvert)
        {
            long ret = 0;
            for (int i = 0; i < bytesToConvert; i++)
            {
                ret = (ret << 8) | value[i];
            }
            return ret;
        }
    }
}

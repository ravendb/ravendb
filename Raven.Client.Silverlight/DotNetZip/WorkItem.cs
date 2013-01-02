namespace Ionic.Zlib
{
	internal class WorkItem
	{
		internal enum Status { None=0, Filling=1, Filled=2, Compressing=3, Compressed=4, Writing=5, Done=6 }
		public byte[] buffer;
		public byte[] compressed;
		public int status;
		public int crc;
		public int index;
		public int inputBytesAvailable;
		public int compressedBytesAvailable;
		public ZlibCodec compressor;

		public WorkItem(int size, Ionic.Zlib.CompressionLevel compressLevel, CompressionStrategy strategy)
		{
			buffer= new byte[size];
			// alloc 5 bytes overhead for every block (margin of safety= 2)
			int n = size + ((size / 32768)+1) * 5 * 2;
			compressed= new byte[n];

			status = (int)Status.None;
			compressor = new ZlibCodec();
			compressor.InitializeDeflate(compressLevel, false);
			compressor.OutputBuffer = compressed;
			compressor.InputBuffer = buffer;
		}
	}
}
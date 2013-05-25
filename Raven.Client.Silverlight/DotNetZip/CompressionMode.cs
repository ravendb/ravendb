namespace Ionic.Zlib
{
	/// <summary>
	/// An enum to specify the direction of transcoding - whether to compress or decompress.
	/// </summary>
	public enum CompressionMode
	{
		/// <summary>
		/// Used to specify that the stream should compress the data.
		/// </summary>
		Compress= 0,
		/// <summary>
		/// Used to specify that the stream should decompress the data.
		/// </summary>
		Decompress = 1,
	}
}
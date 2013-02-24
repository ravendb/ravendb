namespace Ionic.Zlib
{
	/// <summary>
	/// Describes options for how the compression algorithm is executed.  Different strategies
	/// work better on different sorts of data.  The strategy parameter can affect the compression
	/// ratio and the speed of compression but not the correctness of the compresssion.
	/// </summary>
	public enum CompressionStrategy
	{
		/// <summary>
		/// The default strategy is probably the best for normal data. 
		/// </summary>
		Default = 0,

		/// <summary>
		/// The <c>Filtered</c> strategy is intended to be used most effectively with data produced by a
		/// filter or predictor.  By this definition, filtered data consists mostly of small
		/// values with a somewhat random distribution.  In this case, the compression algorithm
		/// is tuned to compress them better.  The effect of <c>Filtered</c> is to force more Huffman
		/// coding and less string matching; it is a half-step between <c>Default</c> and <c>HuffmanOnly</c>.
		/// </summary>
		Filtered = 1,

		/// <summary>
		/// Using <c>HuffmanOnly</c> will force the compressor to do Huffman encoding only, with no
		/// string matching.
		/// </summary>
		HuffmanOnly = 2,
	}
}
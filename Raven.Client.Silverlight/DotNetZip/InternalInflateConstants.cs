namespace Ionic.Zlib
{
	internal static class InternalInflateConstants
	{
		// And'ing with mask[n] masks the lower n bits
		internal static readonly int[] InflateMask = new int[] {
																   0x00000000, 0x00000001, 0x00000003, 0x00000007,
																   0x0000000f, 0x0000001f, 0x0000003f, 0x0000007f,
																   0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff,
																   0x00000fff, 0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff };
	}
}
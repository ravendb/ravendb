namespace Voron.Util
{
	public class Utils
	{
		public static long NearestPowerOfTwo(long v)
		{
			v--;
			v |= v >> 1;
			v |= v >> 2;
			v |= v >> 4;
			v |= v >> 8;
			v |= v >> 16;
			v++;
			return v;

		}
	}
}
using System;

namespace Raven.Json.Utilities
{
    internal class MathUtils
    {
		public static bool ApproxEquals(double d1, double d2)
		{
			// are values equal to within 6 (or so) digits of precision?
			return Math.Abs(d1 - d2) < (Math.Abs(d1) * 1e-6);
		}
    }

}

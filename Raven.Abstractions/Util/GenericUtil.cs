using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Util
{
	public static class GenericUtil
	{
		public static readonly string[] ByteToHexAsStringLookup;

		static GenericUtil()
		{
			ByteToHexAsStringLookup = new string[byte.MaxValue + 1];
			for (int b = Byte.MinValue; b <= Byte.MaxValue; b++)
				ByteToHexAsStringLookup[b] = b.ToString("X2");
		}
	}
}

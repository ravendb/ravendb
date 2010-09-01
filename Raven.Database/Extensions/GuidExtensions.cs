using System;
using System.Text;

namespace Raven.Database.Extensions
{
	public static class GuidExtensions
	{
		public static Guid TransfromToGuidWithProperSorting(this byte[] bytes)
		{
			var input = Encoding.ASCII.GetString(bytes);
			return Guid.ParseExact(input, "N");
		}

		public static string TransformToValueForEsentSorting(this Guid guid)
		{
			return guid.ToString("N");
		}
	}
}
using System.Text;

namespace Raven.Database.Server.RavenFS.Extensions
{
	public static class ByteExtensions
	{
		public static string ToStringHash(this byte[] hash)
		{
			var sb = new StringBuilder();
			for (var i = 0; i < hash.Length; i++)
			{
				sb.Append(hash[i].ToString("x2"));
			}
			return sb.ToString();
		}
	}
}
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;

namespace Raven.Abstractions.Connection
{
	public static class HttpResponseHeadersExtensions
	{
		/// <returns>
		/// Returns <see cref="T:System.Collections.Generic.IEnumerable`1"/>.
		/// </returns>
		public static string GetFirstValue(this HttpHeaders headers, string name)
		{
			IEnumerable<string> values;
			if (!headers.TryGetValues(name, out values))
				return null;
			return values.FirstOrDefault();
		}

		/// <returns>
		/// Returns <see cref="T:System.Collections.Generic.IEnumerable`1"/>.
		/// </returns>
		public static string[] GetAllValues(this HttpHeaders headers, string name)
		{
			IEnumerable<string> values;
			if (!headers.TryGetValues(name, out values))
				return null;
			return values.ToArray();
		}
	}
}
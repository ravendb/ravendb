using System;
using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.WebApi.Attributes
{
	[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
	public sealed class HttpResetAttribute : HttpVerbAttribute
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="HttpResetAttribute" /> class.
		/// </summary>
		public HttpResetAttribute()
			: base(new HttpMethod("RESET"))
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="HttpResetAttribute" /> class.
		/// </summary>
		/// <param name="routeTemplate">The route template describing the URI pattern to match against.</param>
		public HttpResetAttribute(string routeTemplate)
			: base(new HttpMethod("RESET"), routeTemplate)
		{
		}
	}
}
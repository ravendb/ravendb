using System;
using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.WebApi.Attributes
{
	[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
	public sealed class HttpEvalAttribute : HttpVerbAttribute
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="HttpEvalAttribute" /> class.
		/// </summary>
		public HttpEvalAttribute()
			: base(new HttpMethod("EVAL"))
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="HttpEvalAttribute" /> class.
		/// </summary>
		/// <param name="routeTemplate">The route template describing the URI pattern to match against.</param>
		public HttpEvalAttribute(string routeTemplate)
			: base(new HttpMethod("EVAL"), routeTemplate)
		{
		}
	}
}
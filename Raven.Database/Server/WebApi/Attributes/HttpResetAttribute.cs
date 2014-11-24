using System;
using System.Collections.ObjectModel;
using System.Net.Http;
using System.Web.Http.Controllers;

namespace Raven.Database.Server.WebApi.Attributes
{
	[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
	public sealed class HttpResetAttribute : Attribute, IActionHttpMethodProvider
	{
		private static readonly Collection<HttpMethod> SupportedMethods = new Collection<HttpMethod>(new[]
		{
			new HttpMethod("RESET")
		});

		public Collection<HttpMethod> HttpMethods { get { return SupportedMethods; } }
	}
}
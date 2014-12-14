using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;

namespace Raven.Database.Server.WebApi.Attributes
{
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
	public class RavenRouteAttribute : Attribute, IDirectRouteFactory, IHttpRouteInfoProvider
	{
		public RavenRouteAttribute()
		{
			Template = string.Empty;
		}

		public RavenRouteAttribute(string template)
		{
			if (template == null)
				throw new ArgumentNullException("template");

			Template = template;
		}

		public string Name { get; set; }

		public int Order { get; set; }

		public string Template { get; private set; }

		RouteEntry IDirectRouteFactory.CreateRoute(DirectRouteFactoryContext context)
		{
			Contract.Assert(context != null);

			if (context.InlineConstraintResolver is RavenInlineConstraintResolver == false)
				return FakeRouteEntry;

			IDirectRouteBuilder builder = context.CreateBuilder(Template);
			Contract.Assert(builder != null);

			builder.Name = Name;
			builder.Order = Order;

			return builder.Build();
		}

		private static readonly RouteEntry FakeRouteEntry = new RouteEntry(
			null,
			new HttpRoute(
				Guid.NewGuid().ToString("N"),
				new HttpRouteValueDictionary(),
				new HttpRouteValueDictionary(),
				new HttpRouteValueDictionary
				{
					{ "actions", new HttpActionDescriptor[] { new FakeActionDescriptor() } }
				}));

		private class FakeActionDescriptor : HttpActionDescriptor
		{
			public override Collection<HttpParameterDescriptor> GetParameters()
			{
				throw new NotImplementedException();
			}

			public override Task<object> ExecuteAsync(HttpControllerContext controllerContext, IDictionary<string, object> arguments, CancellationToken cancellationToken)
			{
				throw new NotImplementedException();
			}

			public override string ActionName
			{
				get
				{
					throw new NotImplementedException();
				}
			}

			public override Type ReturnType
			{
				get
				{
					throw new NotImplementedException();
				}
			}
		}
	}
}
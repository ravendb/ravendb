using System;
using System.Collections.Generic;
using System.Web.Http.Dependencies;

namespace Raven.Database.Server.RavenFS.Infrastructure
{
	public class DelegateDependencyResolver : IDependencyResolver
	{
		private readonly Func<Type, object> getService;
		private readonly Func<Type, IEnumerable<object>> getServices;

		public DelegateDependencyResolver(Func<Type, object> getService, Func<Type, IEnumerable<object>> getServices)
		{
			this.getService = getService;
			this.getServices = getServices;
		}

		public void Dispose()
		{
		}

		public object GetService(Type serviceType)
		{
			try
			{
				return getService.Invoke(serviceType);
			}
			catch
			{
				return null;
			}
		}

		public IEnumerable<object> GetServices(Type serviceType)
		{
			return getServices(serviceType);
		}

		public IDependencyScope BeginScope()
		{
			return this;
		}
	}
}
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Studio.Features.Collections;

namespace Raven.Studio.Infrastructure.Services
{
	public class UriBinder
	{
		private readonly Dictionary<Type, string> routes;

		public UriBinder()
		{
			routes = new Dictionary<Type, string>();
			routes.Add(typeof(CollectionsViewModel), "~/{database}/collections");
		}

		public Tuple<Type, Dictionary<string, string>> ResolveViewModel(string url)
		{
			Type type = routes.Keys.First();
			var parameters = new Dictionary<string, string>();
			return new Tuple<Type, Dictionary<string, string>>(type, parameters);
		}
	}
}
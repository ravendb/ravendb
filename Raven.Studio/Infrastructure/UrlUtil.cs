using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public class UrlUtil
	{
		static UrlUtil()
		{
			Url = Application.Current.Host.NavigationState;
			Application.Current.Host.NavigationStateChanged += (sender, args) => Url = args.NewNavigationState;
		}

		public static string Url { get; private set; }

		private Dictionary<string, string> queryParams;
		public Dictionary<string, string> QueryParams
		{
			get
			{
				if (queryParams == null)
				{
					queryParams = new Dictionary<string, string>();
					var indexOf = Url.IndexOf('?');
					if (indexOf == -1)
						return queryParams;

					var options = Url.Substring(indexOf + 1).Split(new[] { '&' }, StringSplitOptions.RemoveEmptyEntries);
					foreach (var option in options)
					{
						var param = option.Split(new[] {'='}, StringSplitOptions.RemoveEmptyEntries);
						queryParams.Add(param[0], param[1]);
					}
				}
				return queryParams;
			}
		}

		public string GetQueryParam(string name)
		{
			if (QueryParams.ContainsKey(name))
				return QueryParams[name];
			return null;
		}

		public void SetQueryParam(string name, object value)
		{
			QueryParams[name] = value.ToString();
		}

		public void NavigateTo()
		{
			var indexOf = Url.IndexOf('?');
			var uri = indexOf != -1 ? Url.Substring(0, indexOf) : Url;
			var query = string.Join("&", QueryParams.Select(x => string.Format("{0}={1}", x.Key, x.Value)));
			if (string.IsNullOrEmpty(query) == false)
			{
				uri += "?" + query;
			}
			Navigate(uri);
		}

		private static void Navigate(Uri source)
		{
			if (Deployment.Current.Dispatcher.CheckAccess())
				Application.Current.Host.NavigationState = source.ToString();
			else
				Deployment.Current.Dispatcher.InvokeAsync(() => Application.Current.Host.NavigationState = source.ToString());
		}

		public static void Navigate(string url)
		{
			url = IncludeDatabaseName(url);
			if (url == null)
				return;
			Navigate((new Uri(url, UriKind.Relative)));
		}

		private static string IncludeDatabaseName(string url)
		{
			url = url.Trim();
			if (string.IsNullOrEmpty(url))
				return "/" + ServerModel.DefaultDatabaseName;

			var start = url.StartsWith("/") ? 1 : 0;
			var end = url.IndexOf('/', start + 1);
			var databaseName = end == -1 ? url.Substring(start) : url.Substring(start, end - start);
			var currentDatabaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

			if (databaseName == currentDatabaseName) // Do not redirect the URL twice.
				return null;
			if (ApplicationModel.Current.Server.Value.Databases.Where(x => x.Name == databaseName).Any())
				return url.ReplaceSingle(databaseName, currentDatabaseName);
			return "/" + currentDatabaseName + url;
		}
	}
}
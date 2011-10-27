using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public class UrlUtil
	{
		public string Url { get; private set; }

		public UrlUtil() : this(ApplicationModel.NavigationState) { }

		public UrlUtil(string url)
		{
			Url = url;
		}

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
						return null;

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
			var uri = new UriBuilder(new Uri(Url, UriKind.Relative));
			uri.Query = string.Join("&", QueryParams.Select(x => string.Format("{0}={1}", x.Key, x.Value)));
			ApplicationModel.Current.Navigate(uri.Uri);
		}
	}
}
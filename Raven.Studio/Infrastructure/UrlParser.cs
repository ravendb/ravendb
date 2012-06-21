using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public class UrlParser
	{
		private readonly string url;
		private string Url
		{
			get { return url; }
		}

		private Dictionary<string, string> queryParams;

		public UrlParser(string url)
		{
			this.url = url;
		}

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
						var param = option.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
						queryParams.Add(param[0], param[1]);
					}
				}
				return queryParams;
			}
		}

		private string path;
		public string Path
		{
			get
			{
				if (path == null)
				{
					var indexOf = Url.IndexOf('?');
					path = indexOf != -1 ? Url.Substring(0, indexOf) : Url;
				}
				return path;
			}
		}

		public string GetQueryParam(string name)
		{
			if (QueryParams.ContainsKey(name) == false)
				return null;
			return Uri.UnescapeDataString(QueryParams[name]);
		}

		public void SetQueryParam(string name, object value)
		{
			if (value == null) return;
			QueryParams[name] = Uri.EscapeDataString(value.ToString());
		}

		public bool RemoveQueryParam(string name)
		{
			return QueryParams.Remove(name);
		}

		public string BuildUrl()
		{
			EnsureDatabaseParameterIncluded();

			var uri = Path;
			if (string.IsNullOrWhiteSpace(uri))
				uri = "/home";
			var query = string.Join("&", QueryParams.Select(x => string.Format("{0}={1}", x.Key, x.Value)));
			if (string.IsNullOrEmpty(query) == false)
			{
				uri += "?" + query;
			}
			return uri;
		}

		private void EnsureDatabaseParameterIncluded()
		{
			if (GetQueryParam("database") != null || ApplicationModel.Database.Value == null)
				return;

			SetQueryParam("database", ApplicationModel.Database.Value.Name);
		}
	}
}
using System;
using System.Collections.Generic;
using System.Net;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
#if SILVERLIGHT
using System.Windows.Browser;
using Raven.Client.Silverlight.Connection;
#endif

namespace Raven.Client.Connection
{
	public static class RavenUrlExtensions
	{
		public static string Indexes(this string url, string index)
		{
			return url + "/indexes/" + index;
		}

		public static string IndexDefinition(this string url, string index)
		{
			return url + "/indexes/" + index + "?definition=yes";
		}

		public static string IndexNames(this string url, int start, int pageSize)
		{
			return url + "/indexes/?namesOnly=true&start=" + start + "&pageSize=" + pageSize;
		}

		public static string Stats(this string url)
		{
			return url + "/stats";
		}

		//public static string Static(this string url, string key)
		//{
		//    return url + "/static/" + HttpUtility.UrlEncode(key);
		//}

		public static string Databases(this string url, int pageSize, int start)
		{
			var databases = url + "/databases/?pageSize=" + pageSize;
			return start > 0 ? databases + "&start=" + start : databases;
		}

		public static string SilverlightEnsuresStartup(this string url)
		{
			return url + "/silverlight/ensureStartup";
		}

		public static string Terms(this string url, string index, string field, string fromValue, int pageSize)
		{
			return url + "/terms/" + index + "?field=" + field + "&fromValue=" + fromValue + "&pageSize=" + pageSize;
		}
#if SILVERLIGHT
		public static string Docs(this string url, string key)
		{
			return url + "/docs/" + HttpUtility.UrlEncode(key);
		}
#endif

		public static string Docs(this string url, int start, int pageSize)
		{
			return url + "/docs/?start=" + start + "&pageSize=" + pageSize;
		}

		//public static string DocsStartingWith(this string url, string prefix, int start, int pageSize)
		//{
		//    return Docs(url, start, pageSize) + "&startsWith=" + HttpUtility.UrlEncode(prefix);
		//}

		public static string Queries(this string url)
		{
			return url + "/queries/";
		}

		public static string NoCache(this string url)
		{
#if !SILVERLIGHT 
			return url;
#else
			return (url.Contains("?"))
				? url + "&noCache=" + Guid.NewGuid().GetHashCode()
				: url + "?noCache=" + Guid.NewGuid().GetHashCode();
#endif
		}

		public static Uri ToUri(this string url)
		{
			return new Uri(url);
		}

		public static HttpJsonRequest ToJsonRequest(this string url, AsyncServerClient requestor, ICredentials credentials, Document.DocumentConvention convention)
		{
			return requestor.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(requestor, url, "GET", credentials, convention));
		}

		public static HttpJsonRequest ToJsonRequest(this string url, AsyncServerClient requestor, ICredentials credentials, DocumentConvention convention, IDictionary<string, string> operationsHeaders, string method)
		{
			var httpJsonRequest = requestor.jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(requestor, url, method, credentials, convention)
					.AddOperationHeaders(operationsHeaders));
			
			return httpJsonRequest;
		}
	}
}
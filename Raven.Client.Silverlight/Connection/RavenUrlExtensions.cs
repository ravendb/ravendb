using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Browser;
using System.Windows.Browser;
using Raven.Client.Document;
using Raven.Client.Silverlight.Connection.Async;

namespace Raven.Client.Silverlight.Connection
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

		public static string Static(this string url, string key)
		{
			return url + "/static/" + HttpUtility.HtmlEncode(key);
		}

		public static string Databases(this string url, int pageSize, int start)
		{
			string databases = url + "/databases/?pageSize=" + pageSize;
			if(start > 0)
				return databases + "&start=" + start;
			return databases;
		}

		public static string SilverlightEnsuresStartup(this string url)
		{
			return url + "/silverlight/ensureStartup";
		}

		public static string Terms(this string url, string index, string field, string fromValue, int pageSize)
		{
			return url + "/terms/" + index + "?field=" + field + "&fromValue=" + fromValue + "&pageSize=" + pageSize;
		}

		public static string Docs(this string url, string key)
		{
			return url + "/docs/" + HttpUtility.HtmlEncode(key);
		}

		public static string Docs(this string url, int start, int pageSize)
		{
			return url + "/docs/?start=" + start + "&pageSize=" + pageSize;
		}

		public static string DocsStartingWith(this string url, string prefix, int start, int pageSize)
		{
			return Docs(url, start, pageSize) + "&startsWith=" + HttpUtility.HtmlEncode(prefix);
		}

		public static string Queries(this string url)
		{
			return url + "/queries/";
		}

		public static string NoCache(this string url)
		{
			return (url.Contains("?"))
				? url + "&noCache=" + Guid.NewGuid().GetHashCode()
				: url + "?noCache=" + Guid.NewGuid().GetHashCode();
		}

		public static Uri ToUri(this string url)
		{
			return new Uri(url);
		}

		public static HttpJsonRequest ToJsonRequest(this string url, AsyncServerClient requestor, ICredentials credentials, Document.DocumentConvention convention)
		{
			return requestor.JsonRequestFactory.CreateHttpJsonRequest(requestor, url, "GET", credentials, convention);
		}

		public static HttpJsonRequest ToJsonRequest(this string url, AsyncServerClient requestor, ICredentials credentials, DocumentConvention convention, IDictionary<string, string> operationsHeaders, string method)
		{
			var httpJsonRequest = requestor.JsonRequestFactory.CreateHttpJsonRequest(requestor, url, method, credentials, convention);
			httpJsonRequest.AddOperationHeaders(operationsHeaders);
			return httpJsonRequest;
		}
	}
}
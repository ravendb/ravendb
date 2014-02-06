using System;
using System.Collections.Generic;
using System.Net;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
#if SILVERLIGHT
using System.Windows.Browser;
#endif
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#elif NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif

namespace Raven.Client.Connection
{
    using System.Collections.Specialized;
	using Raven.Abstractions.Connection;

	public static class RavenUrlExtensions
	{
        public static string ForDatabase(this string url, string database)
        {
            if (!string.IsNullOrEmpty(database) && !url.Contains("/databases/"))
                return url + "/databases/" + database;

            return url;
        }

		public static string Indexes(this string url, string index)
		{
			return url + "/indexes/" + index;
		}

		public static string IndexDefinition(this string url, string index)
		{
			return url + "/indexes/" + index + "?definition=yes";
		}

		public static string Transformer(this string url, string transformer)
		{
			return url + "/transformers/" + transformer;
		}

		public static string IndexNames(this string url, int start, int pageSize)
		{
			return url + "/indexes/?namesOnly=true&start=" + start + "&pageSize=" + pageSize;
		}

		public static string Stats(this string url)
		{
			return url + "/stats";
		}

		public static string AdminStats(this string url)
		{
			return url + "/admin/stats";
		}

		public static string ReplicationInfo(this string url)
		{
			return url + "/replication/info";
		}

		public static string LastReplicatedEtagFor(this string destinationUrl, string sourceUrl)
		{
			return destinationUrl + "/replication/lastEtag?from=" + Uri.EscapeDataString(sourceUrl);
		}

		//public static string Static(this string url, string key)
		//{
		//    return url + "/static/" + HttpUtility.UrlEncode(key);
		//}

		public static string Databases(this string url, int pageSize, int start)
		{
			var databases = url + "/databases?pageSize=" + pageSize;
			return start > 0 ? databases + "&start=" + start : databases;
		}

		public static string Terms(this string url, string index, string field, string fromValue, int pageSize)
		{
			return url + "/terms/" + index + "?field=" + field + "&fromValue=" + fromValue + "&pageSize=" + pageSize;
		}

		public static string Doc(this string url, string key)
		{

			return url + "/docs/" 
#if SILVERLIGHT 
				+ HttpUtility.UrlEncode(key)
#else
				+ key
#endif
				;
		}

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

		public static HttpJsonRequest ToJsonRequest(this string url, AsyncServerClient requestor, OperationCredentials credentials, Document.DocumentConvention convention)
		{
			return requestor.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(requestor, url, "GET", credentials, convention));
		}

		public static HttpJsonRequest ToJsonRequest(this string url, AsyncServerClient requestor,
												 OperationCredentials credentials, DocumentConvention convention,
												 NameValueCollection operationsHeaders, string method)
		{
			var httpJsonRequest = requestor.jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(requestor, url, method, credentials, convention)
							.AddOperationHeaders(operationsHeaders));

			return httpJsonRequest;
		}
	}
}

using System;

using Raven.Client.Extensions;

namespace Raven.Client.Connection
{
	public static class RavenUrlExtensions
	{
        public static string ForDatabase(this string url, string database)
        {
	        if (!string.IsNullOrEmpty(database) && !url.Contains("/databases/"))
	        {
		        if (url.EndsWith("/"))
					return url + "databases/" + database;

				return url + "/databases/" + database;
	        }

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

		public static string IndexingPerformanceStatistics(this string url)
		{
			return url + "/debug/indexing-perf-stats";
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
			return MultiDatabase.GetRootDatabaseUrl(url) +"/admin/stats";
		}

		public static string ReplicationInfo(this string url)
		{
			return url + "/replication/info";
		}

		public static string LastReplicatedEtagFor(this string destinationUrl, string sourceUrl, string sourceDbId, string[] sourceCollections = null)
		{
			return destinationUrl + "/replication/lastEtag?from=" + Uri.EscapeDataString(sourceUrl) + "&dbid=" + sourceDbId;
		}

		public static string Databases(this string url, int pageSize, int start)
		{
			var databases = MultiDatabase.GetRootDatabaseUrl(url) +"/databases?pageSize=" + pageSize;
			return start > 0 ? databases + "&start=" + start : databases;
		}

		public static string Terms(this string url, string index, string field, string fromValue, int pageSize)
		{
			return url + "/terms/" + index + "?field=" + field + "&fromValue=" + fromValue + "&pageSize=" + pageSize;
		}

		public static string Doc(this string url, string key)
		{
			return url + "/docs/" + key;
		}

		public static string Docs(this string url, int start, int pageSize)
		{
			return url + "/docs/?start=" + start + "&pageSize=" + pageSize;
		}

		public static string Queries(this string url)
		{
			return url + "/queries/";
		}

		public static Uri ToUri(this string url)
		{
			return new Uri(url);
		}
	}
}

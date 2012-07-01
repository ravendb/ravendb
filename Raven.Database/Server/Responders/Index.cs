//-----------------------------------------------------------------------
// <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using NLog;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using System.Linq;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Queries;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;

namespace Raven.Database.Server.Responders
{
	public class Index : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/indexes/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "PUT", "DELETE","HEAD","RESET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;
			
			switch (context.Request.HttpMethod)
			{
				case "HEAD":
					if(Database.IndexDefinitionStorage.IndexNames.Contains(index, StringComparer.InvariantCultureIgnoreCase) == false)
						context.SetStatusToNotFound();
					break;
				case "GET":
					OnGet(context, index);
					break;
				case "PUT":
					Put(context, index);
					break;
				case "RESET":
					Database.ResetIndex(index);
					context.WriteJson(new {Reset = index});
					break;
				case "DELETE":
					context.SetStatusToDeleted();
					Database.DeleteIndex(index);
					break;
			}
		}

		private void Put(IHttpContext context, string index)
		{
			var data = context.ReadJsonObject<IndexDefinition>();
			if (data == null || (data.Map == null && (data.Maps == null || data.Maps.Count == 0)))
			{
				context.SetStatusToBadRequest();
				context.Write("Expected json document with 'Map' or 'Maps' property");
				return;
			}
			context.SetStatusToCreated("/indexes/" + index);
			context.WriteJson(new { Index = Database.PutIndex(index, data) });
		}

		private void OnGet(IHttpContext context, string index)
		{
			if (string.IsNullOrEmpty(context.Request.QueryString["definition"]) == false)
			{
				GetIndexDefinition(context, index);
			}
			else if (string.IsNullOrEmpty(context.Request.QueryString["source"]) == false)
			{
				GetIndexSource(context, index);
			}
			else if (string.IsNullOrEmpty(context.Request.QueryString["mapresults"]) == false)
			{
				GetIndexMappedResult(context, index);
			}
			else 
			{
				GetIndexQueryRessult(context, index);
			}
		}

		private void GetIndexMappedResult(IHttpContext context, string index)
		{
			if(Database.IndexDefinitionStorage.GetIndexDefinition(index)==null)
			{
				context.SetStatusToNotFound();
				return;
			}

			var etag = context.GetEtagFromQueryString() ?? Guid.Empty;
			List<MappedResultInfo> mappedResult = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				mappedResult = accessor.MappedResults.GetMappedResultsReduceKeysAfter(index, etag, 
					loadData: true, 
					take: context.GetPageSize(Settings.MaxPageSize))
					.ToList();
			});
			context.WriteJson(mappedResult);
		}

		private void GetIndexQueryRessult(IHttpContext context, string index)
		{
			Guid indexEtag;

			var queryResult = ExecuteQuery(context, index, out indexEtag);

			if (queryResult == null)
				return;

			var includes = context.Request.QueryString.GetValues("include") ?? new string[0];
			var loadedIds = new HashSet<string>(
				queryResult.Results
					.Where(x => x["@metadata"] != null)
					.Select(x => x["@metadata"].Value<string>("@id"))
					.Where(x => x != null)
				);
			var command = new AddIncludesCommand(Database, GetRequestTransaction(context),
			                                     (etag, doc) => queryResult.Includes.Add(doc), includes, loadedIds);
			foreach (var result in queryResult.Results)
			{
				command.Execute(result);
			}
			command.AlsoInclude(queryResult.IdsToInclude);

			context.WriteETag(indexEtag);
			if(queryResult.NonAuthoritativeInformation)
				context.SetStatusToNonAuthoritativeInformation();
			context.WriteJson(queryResult);
		}

		private void GetIndexDefinition(IHttpContext context, string index)
		{
			var indexDefinition = Database.GetIndexDefinition(index);
			if(indexDefinition == null)
			{
				context.SetStatusToNotFound();
				return;
			}

			indexDefinition.Fields = Database.GetIndexFields(index);

			context.WriteJson(new
			{
				Index = indexDefinition,
			});
		}

		private void GetIndexSource(IHttpContext context, string index)
		{
			var viewGenerator = Database.IndexDefinitionStorage.GetViewGenerator(index);
			if (viewGenerator == null)
			{
				context.SetStatusToNotFound();
				return;
			}

			context.Write(viewGenerator.SourceCode);
		}

		private QueryResultWithIncludes ExecuteQuery(IHttpContext context, string index, out Guid indexEtag)
		{
			var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);

			RewriteDateQueriesFromOldClients(context,indexQuery);

			var sp = Stopwatch.StartNew();
			var result = index.StartsWith("dynamic/", StringComparison.InvariantCultureIgnoreCase) || index.Equals("dynamic", StringComparison.InvariantCultureIgnoreCase) ? 
				PerformQueryAgainstDynamicIndex(context, index, indexQuery, out indexEtag) : 
				PerformQueryAgainstExistingIndex(context, index, indexQuery, out indexEtag);

			sp.Stop();

			context.Log(log => log.Debug(() =>
			{
				var sb = new StringBuilder("\tQuery: ")
					.Append(indexQuery.Query)
					.AppendLine();
				sb.Append("\t").AppendFormat("Time: {0:#,#;;0} ms", sp.ElapsedMilliseconds).AppendLine();

				if (result == null)
					return sb.ToString();

				sb.Append("\tIndex: ")
					.AppendLine(result.IndexName);
				sb.Append("\t").AppendFormat("Results: {0:#,#;;0} returned out of {1:#,#;;0} total.", result.Results.Count, result.TotalResults).AppendLine();

				return sb.ToString();
			}));

			return result;
		}

		static Regex oldDateTimeFormat = new Regex(@"(\:|\[|TO\s) \s* (\d{17})", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private void RewriteDateQueriesFromOldClients(IHttpContext context,IndexQuery indexQuery)
		{
			var clientVersion = context.Request.Headers["Raven-Client-Version"];
			if (string.IsNullOrEmpty(clientVersion) == false) // new client
				return;

			var matches = oldDateTimeFormat.Matches(indexQuery.Query);
			if (matches.Count == 0)
				return;
			var builder = new StringBuilder(indexQuery.Query);
			for (int i = matches.Count-1; i >= 0; i--) // working in reverse so as to avoid invalidating previous indexes
			{
				var dateTimeString = matches[i].Groups[2].Value;

				DateTime time;
				if (DateTime.TryParseExact(dateTimeString, "yyyyMMddHHmmssfff", CultureInfo.InvariantCulture, DateTimeStyles.None, out time) == false)
					continue;

				builder.Remove(matches[i].Groups[2].Index, matches[i].Groups[2].Length);
				var newDateTimeFormat = time.ToString(Default.DateTimeFormatsToWrite);
				builder.Insert(matches[i].Groups[2].Index, newDateTimeFormat);
			}
			indexQuery.Query = builder.ToString();
		}

		private QueryResultWithIncludes PerformQueryAgainstExistingIndex(IHttpContext context, string index, IndexQuery indexQuery, out Guid indexEtag)
		{
			indexEtag = Database.GetIndexEtag(index, null);
			if (context.MatchEtag(indexEtag))
			{
				context.SetStatusToNotModified();
				return null;
			}

			var queryResult = Database.Query(index, indexQuery);
			indexEtag = Database.GetIndexEtag(index, queryResult.ResultEtag);
			return queryResult;
		}

		private QueryResultWithIncludes PerformQueryAgainstDynamicIndex(IHttpContext context, string index, IndexQuery indexQuery, out Guid indexEtag)
		{
			string entityName = null;
			if (index.StartsWith("dynamic/", StringComparison.InvariantCultureIgnoreCase))
				entityName = index.Substring("dynamic/".Length);

			var dynamicIndexName = Database.FindDynamicIndexName(entityName, indexQuery);

			if (dynamicIndexName != null && 
				Database.IndexStorage.HasIndex(dynamicIndexName))
			{
				indexEtag = Database.GetIndexEtag(dynamicIndexName, null);
				if (context.MatchEtag(indexEtag))
				{
					context.SetStatusToNotModified();
					return null;
				}
			}

			var queryResult = Database.ExecuteDynamicQuery(entityName, indexQuery);

			// have to check here because we might be getting the index etag just 
			// as we make a switch from temp to auto, and we need to refresh the etag
			// if that is the case. This can also happen when the optmizer
			// decided to switch indexes for a query.
			indexEtag = (dynamicIndexName  == null || queryResult.IndexName == dynamicIndexName) ?
				Database.GetIndexEtag(queryResult.IndexName, queryResult.ResultEtag) : 
				Guid.NewGuid();

			return queryResult;
		}

		
	}
}

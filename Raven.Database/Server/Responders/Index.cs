//-----------------------------------------------------------------------
// <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Data;
using Raven.Database.Indexing;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Queries;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

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
					if(index.StartsWith("Raven/",StringComparison.InvariantCultureIgnoreCase))
					{
						context.SetStatusToForbidden();
						context.WriteJson(new
						{
							Url = context.Request.RawUrl,
							Error = "Builtin indexes cannot be deleted, attempt to delete index '" + index + "' was rejected"
						});
						return;
					}
					context.SetStatusToDeleted();
					Database.DeleteIndex(index);
					break;
			}
		}

		private void Put(IHttpContext context, string index)
		{
			var data = context.ReadJsonObject<IndexDefinition>();
			if (data == null || data.Map == null)
			{
				context.SetStatusToBadRequest();
				context.Write("Expected json document with 'Map' property");
				return;
			}
			context.SetStatusToCreated("/indexes/" + index);
			context.WriteJson(new { Index = Database.PutIndex(index, data) });
		}

		private void OnGet(IHttpContext context, string index)
		{
			var definition = context.Request.QueryString["definition"];
			if ("yes".Equals(definition, StringComparison.InvariantCultureIgnoreCase))
			{
				GetIndexDefinition(context, index);
			}
			else
			{
				GetIndexQueryRessult(context, index);
			}
		}

		private void GetIndexQueryRessult(IHttpContext context, string index)
		{
			Guid indexEtag;

			QueryResult queryResult = ExecuteQuery(context, index, out indexEtag);

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
			
			context.Response.AddHeader("ETag", indexEtag.ToString());
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

		private QueryResult ExecuteQuery(IHttpContext context, string index, out Guid indexEtag)
		{
			var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);

			return index.StartsWith("dynamic", StringComparison.InvariantCultureIgnoreCase) ? 
				PerformQueryAgainstDynamicIndex(context, index, indexQuery, out indexEtag) : 
				PerformQueryAgainstExistingIndex(context, index, indexQuery, out indexEtag);
		}

		private QueryResult PerformQueryAgainstExistingIndex(IHttpContext context, string index, IndexQuery indexQuery, out Guid indexEtag)
		{
			indexEtag = GetIndexEtag(index);
			if (context.MatchEtag(indexEtag))
			{
				context.SetStatusToNotModified();
				return null;
			}

			return Database.Query(index, indexQuery);
		}

		private QueryResult PerformQueryAgainstDynamicIndex(IHttpContext context, string index, IndexQuery indexQuery, out Guid indexEtag)
		{
			string entityName = null;
			if (index.StartsWith("dynamic/"))
				entityName = index.Substring("dynamic/".Length);

			var dynamicIndexName = Database.FindDynamicIndexName(entityName, indexQuery.Query);


			if (Database.IndexStorage.HasIndex(dynamicIndexName))
			{
				indexEtag = GetIndexEtag(dynamicIndexName);
				if (context.MatchEtag(indexEtag))
				{
					context.SetStatusToNotModified();
					return null;
				}
			}
			else
			{
				indexEtag = Guid.Empty;
			}

			var queryResult = Database.ExecuteDynamicQuery(entityName, indexQuery);

			// have to check here because we might be getting the index etag just 
			// as we make a switch from temp to auto, and we need to refresh the etag
			// if that is the case. This can also happen when the optmizer
			// decided to switch indexes for a query.
			if (queryResult.IndexName != dynamicIndexName)
				indexEtag = GetIndexEtag(queryResult.IndexName);

			return queryResult;
		}

		private Guid GetIndexEtag(string indexName)
		{
			Guid lastDocEtag = Guid.Empty;
			bool isStale = false;
			Tuple<DateTime, Guid> indexLastUpdatedAt = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				isStale = accessor.Staleness.IsIndexStale(indexName, null, null);
				lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				indexLastUpdatedAt = accessor.Staleness.IndexLastUpdatedAt(indexName);
			});
			var indexDefinition = Database.GetIndexDefinition(indexName);
			using(var md5 = MD5.Create())
			{
				var list = new List<byte>();
				list.AddRange(indexDefinition.GetIndexHash());
				list.AddRange(Encoding.Unicode.GetBytes(indexName));
				list.AddRange(lastDocEtag.ToByteArray());
				list.AddRange(indexLastUpdatedAt.Item2.ToByteArray());
				list.AddRange(BitConverter.GetBytes(isStale));
				return new Guid(md5.ComputeHash(list.ToArray()));
			}
		}
	}
}

//-----------------------------------------------------------------------
// <copyright file="MoreLikeThisResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Json.Linq;

namespace Raven.Bundles.MoreLikeThis
{
	public class MoreLikeThisResponder : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/morelikethis/([\w\-_]+)/(.+)"; } // /morelikethis/(index-name)/(ravendb-document-id)
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var indexName = match.Groups[1].Value;
			var documentId = match.Groups[2].Value;

			var fieldNames = context.Request.QueryString.GetValues("fields");

			if (fieldNames == null || fieldNames.Length == 0)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new { Error = "The query string must specify the fields to check" });
				return;
			}

			var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(indexName);
			if (indexDefinition == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new {Error = "The index " + indexName + " cannot be found"});
				return;
			}


			PerformSearch(context, indexName, fieldNames, documentId, indexDefinition);
		}

		private void PerformSearch(IHttpContext context, string indexName, string[] fieldNames, string documentId,
		                           IndexDefinition indexDefinition)
		{
			IndexSearcher searcher;
			using (Database.IndexStorage.GetCurrentIndexSearcher(indexName, out searcher))
			{
				var td = searcher.Search(new TermQuery(new Term(Constants.DocumentIdFieldName, documentId)), 1);
					// get the current Lucene docid for the given RavenDB doc ID
				if (td.ScoreDocs.Length == 0)
				{
					context.SetStatusToNotFound();
					context.WriteJson(new {Error = "Document " + documentId + " could not be found"});
					return;
				}
				var mlt = new Similarity.Net.MoreLikeThis(searcher.GetIndexReader());

				mlt.SetAnalyzer(indexDefinition.GetAnalyzer(fieldNames[0])); // we use the analyzer of the first speciifed field

				mlt.SetFieldNames(fieldNames);

				var mltQuery = mlt.Like(td.ScoreDocs[0].doc);
				var tsdc = TopScoreDocCollector.create(context.GetPageSize(Database.Configuration.MaxPageSize), true);
				searcher.Search(mltQuery, tsdc);
				var hits = tsdc.TopDocs().ScoreDocs;

				var documentIds = hits.Select(hit => searcher.Doc(hit.doc).Get(Constants.DocumentIdFieldName))
					.Distinct();

				var jsonDocuments =
					documentIds
						.Select(docId => Database.Get(docId, null))
						.Where(it => it != null)
						.ToArray();

				var result = new MultiLoadResult();

				var includedEtags = new List<byte>(jsonDocuments.SelectMany(x => x.Etag.Value.ToByteArray()));
				includedEtags.AddRange(Database.GetIndexEtag(indexName).ToByteArray());
				var loadedIds = new HashSet<string>(jsonDocuments.Select(x=>x.Key));
				var addIncludesCommand = new AddIncludesCommand(Database, GetRequestTransaction(context), (etag, includedDoc) =>
				{
					includedEtags.AddRange(etag.ToByteArray());
					result.Includes.Add(includedDoc);
				}, context.Request.QueryString.GetValues("include"), loadedIds);

				foreach (var jsonDocumet in jsonDocuments)
				{
					addIncludesCommand.Execute(jsonDocumet.DataAsJson);
				}

				Guid computedEtag;

				using (var md5 = MD5.Create())
				{
					var computeHash = md5.ComputeHash(includedEtags.ToArray());
					computedEtag = new Guid(computeHash);
				}

				if (context.MatchEtag(computedEtag))
				{
					context.SetStatusToNotModified();
					return;
				}

				context.Response.AddHeader("ETag", computedEtag.ToString());
				context.WriteJson(result);
			}
		}
	}
}

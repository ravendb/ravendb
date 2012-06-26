//-----------------------------------------------------------------------
// <copyright file="MoreLikeThisResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Linq.PrivateExtensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Constants = Raven.Abstractions.Data.Constants;
using Index = Raven.Database.Indexing.Index;

namespace Raven.Bundles.MoreLikeThis
{
	public class MoreLikeThisResponder : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/morelikethis/?(.+)"; } // /morelikethis/?index={index-name}&docid={ravendb-document-id}
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var parameters = MoreLikeThisQueryParameters.GetParametersFromPath(context.GetRequestUrl(), context.Request.QueryString);
            
			var index = Database.IndexStorage.GetIndexInstance(parameters.IndexName);
			if (index == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new { Error = "The index " + parameters.IndexName + " cannot be found" });
				return;
			}

			if (string.IsNullOrEmpty(parameters.DocumentId) && parameters.MapGroupFields.Count == 0)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new { Error = "The document id or map group fields are mandatory" });
				return;
			}

			PerformSearch(context, parameters.IndexName, index, parameters);
		}

		private void PerformSearch(IHttpContext context, string indexName, Index index, MoreLikeThisQueryParameters parameters)
		{
			IndexSearcher searcher;
			using (Database.IndexStorage.GetCurrentIndexSearcher(indexName, out searcher))
			{
				var documentQuery = new BooleanQuery();

				if (!string.IsNullOrEmpty(parameters.DocumentId))
				{
					documentQuery.Add(new TermQuery(new Term(Constants.DocumentIdFieldName, parameters.DocumentId.ToLowerInvariant())),
					                  BooleanClause.Occur.MUST);
				}

				foreach (string key in parameters.MapGroupFields.Keys)
				{
					documentQuery.Add(new TermQuery(new Term(key, parameters.MapGroupFields[key])),
					                  BooleanClause.Occur.MUST);
				}

				var td = searcher.Search(documentQuery, 1);

				// get the current Lucene docid for the given RavenDB doc ID
				if (td.ScoreDocs.Length == 0)
				{
					context.SetStatusToNotFound();
					context.WriteJson(new {Error = "Document " + parameters.DocumentId + " could not be found"});
					return;
				}

				var ir = searcher.GetIndexReader();
				var mlt = new RavenMoreLikeThis(ir);

				AssignParameters(mlt, parameters);

				if (!string.IsNullOrWhiteSpace(parameters.StopWordsDocumentId))
				{
					var stopWordsDoc = Database.Get(parameters.StopWordsDocumentId, null);
					if (stopWordsDoc == null)
					{
						context.SetStatusToNotFound();
						context.WriteJson(
							new
							{
								Error = "Stop words document " + parameters.StopWordsDocumentId + " could not be found"
							});
						return;
					}
					var stopWords = stopWordsDoc.DataAsJson.JsonDeserialization<StopWordsSetup>().StopWords;
					mlt.SetStopWords(new Hashtable(stopWords.ToDictionary(x => x.ToLower())));
				}

				var fieldNames = parameters.Fields ?? GetFieldNames(ir);
				mlt.SetFieldNames(fieldNames);

				var toDispose = new List<Action>();
				PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = null;
				try
				{
					perFieldAnalyzerWrapper = index.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
					mlt.SetAnalyzer(perFieldAnalyzerWrapper);

					var mltQuery = mlt.Like(td.ScoreDocs[0].doc);
					var tsdc = TopScoreDocCollector.create(context.GetPageSize(Database.Configuration.MaxPageSize), true);
					searcher.Search(mltQuery, tsdc);
					var hits = tsdc.TopDocs().ScoreDocs;
					var jsonDocuments = GetJsonDocuments(parameters, searcher, index, indexName, hits, td.ScoreDocs[0].doc);

					var result = new MultiLoadResult();

					var includedEtags = new List<byte>(jsonDocuments.SelectMany(x => x.Etag.Value.ToByteArray()));
					includedEtags.AddRange(Database.GetIndexEtag(indexName, null).ToByteArray());
					var loadedIds = new HashSet<string>(jsonDocuments.Select(x => x.Key));
					var addIncludesCommand = new AddIncludesCommand(Database, GetRequestTransaction(context), (etag, includedDoc) =>
					{
						includedEtags.AddRange(etag.ToByteArray());
						result.Includes.Add(includedDoc);
					}, context.Request.QueryString.GetValues("include") ?? new string[0], loadedIds);

					foreach (var jsonDocumet in jsonDocuments)
					{
						result.Results.Add(jsonDocumet.ToJson());
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
				finally
				{
					if (perFieldAnalyzerWrapper != null)
						perFieldAnalyzerWrapper.Close();
					foreach (var action in toDispose)
					{
						action();
					}
				}
			}
		}

		private IEnumerable<JsonDocument> GetJsonDocuments(
			MoreLikeThisQueryParameters parameters, IndexSearcher searcher, Index index,
			string indexName, IEnumerable<ScoreDoc> hits, int baseDocId)
		{
			if (string.IsNullOrEmpty(parameters.DocumentId) == false)
			{
				var documentIds = hits
					.Where(hit => hit.doc != baseDocId)
					.Select(hit => searcher.Doc(hit.doc).Get(Constants.DocumentIdFieldName))
					.Where(x => x != null)
					.Distinct();

				return documentIds
					.Select(docId => Database.Get(docId, null))
					.Where(it => it != null)
					.ToArray();
			}

			var fields = searcher.Doc(baseDocId).GetFields().Cast<AbstractField>().Select(x=>x.Name()).Distinct().ToArray();
			var etag = Database.GetIndexEtag(indexName, null);
			return hits
				.Where(hit => hit.doc != baseDocId)
				.Select(hit => new JsonDocument
				{
					DataAsJson = Index.CreateDocumentFromFields(searcher.Doc(hit.doc), new FieldsToFetch(fields, AggregationOperation.None,
						index.IsMapReduce ? Constants.ReduceKeyFieldName : Constants.DocumentIdFieldName)),
					Etag = etag
				})
				.ToArray();

		}

		private static void AssignParameters(Similarity.Net.MoreLikeThis mlt, MoreLikeThisQueryParameters parameters)
		{
			if (parameters.Boost != null) mlt.SetBoost(parameters.Boost.Value);
			if (parameters.MaximumNumberOfTokensParsed != null)
				mlt.SetMaxNumTokensParsed(parameters.MaximumNumberOfTokensParsed.Value);
			if (parameters.MaximumNumberOfTokensParsed != null) mlt.SetMaxNumTokensParsed(parameters.MaximumNumberOfTokensParsed.Value);
			if (parameters.MaximumQueryTerms != null) mlt.SetMaxQueryTerms(parameters.MaximumQueryTerms.Value);
			if (parameters.MaximumWordLength != null) mlt.SetMaxWordLen(parameters.MaximumWordLength.Value);
			if (parameters.MinimumDocumentFrequency != null) mlt.SetMinDocFreq(parameters.MinimumDocumentFrequency.Value);
			if (parameters.MinimumTermFrequency != null) mlt.SetMinTermFreq(parameters.MinimumTermFrequency.Value);
			if (parameters.MinimumWordLength != null) mlt.SetMinWordLen(parameters.MinimumWordLength.Value);
		}

		private static string[] GetFieldNames(IndexReader indexReader)
		{
			var fields = indexReader.GetFieldNames(IndexReader.FieldOption.INDEXED);
			return fields
				.Where(x => x != Constants.DocumentIdFieldName && x != Constants.ReduceKeyFieldName)
				.ToArray();
		}
	}
}

//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Bundles.MoreLikeThis;
using Raven.Database.Indexing;
using Raven.Database.Server.Responders;
using Index = Raven.Database.Indexing.Index;

namespace Raven.Database.Queries
{
	public class MoreLikeThisQueryRunner
	{
		private readonly DocumentDatabase database;

		public MoreLikeThisQueryRunner(DocumentDatabase database)
		{
			this.database = database;
		}

		public MoreLikeThisQueryResult ExecuteMoreLikeThisQuery(MoreLikeThisQuery query, TransactionInformation transactionInformation, int pageSize = 25, string[] include = null)
		{
			if (query == null) throw new ArgumentNullException("query");

			var index = database.IndexStorage.GetIndexInstance(query.IndexName);
			if (index == null)
				throw new InvalidOperationException("The index " + query.IndexName + " cannot be found");

			if (string.IsNullOrEmpty(query.DocumentId) && query.MapGroupFields.Count == 0)
				throw new InvalidOperationException("The document id or map group fields are mandatory");

			IndexSearcher searcher;
			using (database.IndexStorage.GetCurrentIndexSearcher(index.indexId, out searcher))
			{
				var documentQuery = new BooleanQuery();

				if (string.IsNullOrEmpty(query.DocumentId) == false)
				{
					documentQuery.Add(new TermQuery(new Term(Constants.DocumentIdFieldName, query.DocumentId.ToLowerInvariant())), Occur.MUST);
				}

				foreach (string key in query.MapGroupFields.Keys)
				{
					documentQuery.Add(new TermQuery(new Term(key, query.MapGroupFields[key])), Occur.MUST);
				}

				var td = searcher.Search(documentQuery, 1);

				// get the current Lucene docid for the given RavenDB doc ID
				if (td.ScoreDocs.Length == 0)
					throw new InvalidOperationException("Document " + query.DocumentId + " could not be found");

				var ir = searcher.IndexReader;
				var mlt = new RavenMoreLikeThis(ir);

				AssignParameters(mlt, query);

				if (string.IsNullOrWhiteSpace(query.StopWordsDocumentId) == false)
				{
					var stopWordsDoc = database.Get(query.StopWordsDocumentId, null);
					if (stopWordsDoc == null)
						throw new InvalidOperationException("Stop words document " + query.StopWordsDocumentId + " could not be found");

					var stopWordsSetup = stopWordsDoc.DataAsJson.JsonDeserialization<StopWordsSetup>();
					if (stopWordsSetup.StopWords != null)
					{
						var stopWords = stopWordsSetup.StopWords;
						var ht = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
						foreach (var stopWord in stopWords)
						{
							ht.Add(stopWord);
						}
						mlt.SetStopWords(ht);
					}
				}

				var fieldNames = query.Fields ?? GetFieldNames(ir);
				mlt.SetFieldNames(fieldNames);

				var toDispose = new List<Action>();
				RavenPerFieldAnalyzerWrapper perFieldAnalyzerWrapper = null;
				try
				{
					perFieldAnalyzerWrapper = index.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
					mlt.Analyzer = perFieldAnalyzerWrapper;

					var mltQuery = mlt.Like(td.ScoreDocs[0].Doc);
					var tsdc = TopScoreDocCollector.Create(pageSize, true);
					searcher.Search(mltQuery, tsdc);
					var hits = tsdc.TopDocs().ScoreDocs;
					var jsonDocuments = GetJsonDocuments(query, searcher, index, query.IndexName, hits, td.ScoreDocs[0].Doc);

					var result = new MultiLoadResult();

					var includedEtags = new List<byte>(jsonDocuments.SelectMany(x => x.Etag.ToByteArray()));
					includedEtags.AddRange(database.GetIndexEtag(query.IndexName, null).ToByteArray());
					var loadedIds = new HashSet<string>(jsonDocuments.Select(x => x.Key));
					var addIncludesCommand = new AddIncludesCommand(database, transactionInformation, (etag, includedDoc) =>
					{
						includedEtags.AddRange(etag.ToByteArray());
						result.Includes.Add(includedDoc);
					}, include ?? new string[0], loadedIds);

					foreach (var jsonDocument in jsonDocuments)
					{
						result.Results.Add(jsonDocument.ToJson());
						addIncludesCommand.Execute(jsonDocument.DataAsJson);
					}

					Etag computedEtag;
					using (var md5 = MD5.Create())
					{
						var computeHash = md5.ComputeHash(includedEtags.ToArray());
						computedEtag = Etag.Parse(computeHash);
					}

					return new MoreLikeThisQueryResult
					{
						Etag = computedEtag,
						Result = result,
					};
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

		private JsonDocument[] GetJsonDocuments(MoreLikeThisQuery parameters, IndexSearcher searcher, Index index, string indexName, IEnumerable<ScoreDoc> hits, int baseDocId)
		{
			if (string.IsNullOrEmpty(parameters.DocumentId) == false)
			{
				var documentIds = hits
					.Where(hit => hit.Doc != baseDocId)
					.Select(hit => searcher.Doc(hit.Doc).Get(Constants.DocumentIdFieldName))
					.Where(x => x != null)
					.Distinct();

				return documentIds
					.Select(docId => database.Get(docId, null))
					.Where(it => it != null)
					.ToArray();
			}

			var fields = searcher.Doc(baseDocId).GetFields().Cast<AbstractField>().Select(x => x.Name).Distinct().ToArray();
			var etag = database.GetIndexEtag(indexName, null);
			return hits
				.Where(hit => hit.Doc != baseDocId)
				.Select(hit => new JsonDocument
				{
					DataAsJson = Index.CreateDocumentFromFields(searcher.Doc(hit.Doc),
					                                            new FieldsToFetch(fields, false, index.IsMapReduce ? Constants.ReduceKeyFieldName : Constants.DocumentIdFieldName)),
					Etag = etag
				})
				.ToArray();
		}

		private static void AssignParameters(Lucene.Net.Search.Similar.MoreLikeThis mlt, MoreLikeThisQuery parameters)
		{
			if (parameters.Boost != null) mlt.Boost = parameters.Boost.Value;
			if (parameters.BoostFactor != null) mlt.BoostFactor = parameters.BoostFactor.Value;
			if (parameters.MaximumNumberOfTokensParsed != null) mlt.MaxNumTokensParsed = parameters.MaximumNumberOfTokensParsed.Value;
			if (parameters.MaximumQueryTerms != null) mlt.MaxQueryTerms = parameters.MaximumQueryTerms.Value;
			if (parameters.MinimumWordLength != null) mlt.MinWordLen = parameters.MinimumWordLength.Value;
			if (parameters.MaximumWordLength != null) mlt.MaxWordLen = parameters.MaximumWordLength.Value;
			if (parameters.MinimumTermFrequency != null) mlt.MinTermFreq = parameters.MinimumTermFrequency.Value;
			if (parameters.MinimumDocumentFrequency != null) mlt.MinDocFreq = parameters.MinimumDocumentFrequency.Value;
			if (parameters.MaximumDocumentFrequency != null) mlt.MaxDocFreq = parameters.MaximumDocumentFrequency.Value;
			if (parameters.MaximumDocumentFrequencyPercentage != null) mlt.SetMaxDocFreqPct(parameters.MaximumDocumentFrequencyPercentage.Value);
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
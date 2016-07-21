//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Bundles.MoreLikeThis;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Json.Linq;

using Index = Raven.Database.Indexing.Index;

namespace Raven.Database.Queries
{
    public class MoreLikeThisQueryRunner
    {
        private readonly DocumentDatabase database;

        private HashSet<string> idsToLoad;

        private DocumentRetriever documentRetriever;

        public MoreLikeThisQueryRunner(DocumentDatabase database)
        {
            this.database = database;
        }

        public MoreLikeThisQueryResult ExecuteMoreLikeThisQuery(MoreLikeThisQuery query, TransactionInformation transactionInformation, int pageSize = 25)
        {
            if (query == null) throw new ArgumentNullException("query");

            var index = database.IndexStorage.GetIndexInstance(query.IndexName);
            if (index == null)
                throw new InvalidOperationException("The index " + query.IndexName + " cannot be found");

            if (string.IsNullOrEmpty(query.DocumentId) && query.MapGroupFields.Count == 0 && string.IsNullOrEmpty(query.Document))
                throw new InvalidOperationException("The document id, map group fields or document are mandatory");

            IndexSearcher searcher;
            using (database.IndexStorage.GetCurrentIndexSearcher(index.indexId, out searcher))
            {
                int? baseDocId = null;
                if (string.IsNullOrEmpty(query.DocumentId) == false || query.MapGroupFields.Count > 0)
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

                    baseDocId = td.ScoreDocs[0].Doc;
                }

                var ir = searcher.IndexReader;
                var mlt = new RavenMoreLikeThis(ir);

                AssignParameters(mlt, query);

                if (string.IsNullOrWhiteSpace(query.StopWordsDocumentId) == false)
                {
                    var stopWordsDoc = database.Documents.Get(query.StopWordsDocumentId, null);
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
                    var defaultAnalyzer = !string.IsNullOrWhiteSpace(query.DefaultAnalyzerName)
                        ? IndexingExtensions.CreateAnalyzerInstance(Constants.AllFields, query.DefaultAnalyzerName)
                        : new LowerCaseKeywordAnalyzer();
                    perFieldAnalyzerWrapper = index.CreateAnalyzer(defaultAnalyzer, toDispose, true);
                    mlt.Analyzer = perFieldAnalyzerWrapper;

                    var mltQuery = baseDocId.HasValue
                        ? mlt.Like(baseDocId.Value)
                        : mlt.Like(RavenJObject.Parse(query.Document));

                    var tsdc = TopScoreDocCollector.Create(pageSize, true);


                    if (string.IsNullOrWhiteSpace(query.AdditionalQuery) == false)
                    {
                        var additionalQuery = QueryBuilder.BuildQuery(query.AdditionalQuery, perFieldAnalyzerWrapper);
                        mltQuery = new BooleanQuery
                        {
                            {mltQuery, Occur.MUST},
                            {additionalQuery, Occur.MUST},
                        };
                    }

                    searcher.Search(mltQuery, tsdc);
                    var hits = tsdc.TopDocs().ScoreDocs;
                    var jsonDocuments = baseDocId.HasValue
                        ? GetJsonDocuments(query, searcher, index, query.IndexName, hits, baseDocId.Value)
                        : GetJsonDocuments(searcher, index, query.IndexName, hits);

                    var result = new MultiLoadResult();

                    var includedEtags = new List<byte>(jsonDocuments.SelectMany(x => x.Etag.ToByteArray()));
                    includedEtags.AddRange(database.Indexes.GetIndexEtag(query.IndexName, null).ToByteArray());
                    var loadedIds = new HashSet<string>(jsonDocuments.Select(x => x.Key));
                    var addIncludesCommand = new AddIncludesCommand(database, transactionInformation, (etag, includedDoc) =>
                    {
                        includedEtags.AddRange(etag.ToByteArray());
                        result.Includes.Add(includedDoc);
                    }, query.Includes ?? new string[0], loadedIds);

                    idsToLoad = new HashSet<string>();

                    database.TransactionalStorage.Batch(actions =>
                    {
                        documentRetriever = new DocumentRetriever(database.Configuration, actions, database.ReadTriggers, query.TransformerParameters, idsToLoad);

                        using (new CurrentTransformationScope(database, documentRetriever))
                        {
                            foreach (var document in ProcessResults(query, jsonDocuments, database.WorkContext.CancellationToken))
                            {
                                result.Results.Add(document);
                                addIncludesCommand.Execute(document);
                            }
                        }
                    });

                    addIncludesCommand.AlsoInclude(idsToLoad);

                    var computeHash = Encryptor.Current.Hash.Compute16(includedEtags.ToArray());
                    Etag computedEtag = Etag.Parse(computeHash);

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

        private IEnumerable<RavenJObject> ProcessResults(MoreLikeThisQuery query, IEnumerable<JsonDocument> documents, CancellationToken token)
        {
            IndexingFunc transformFunc = null;

            if (string.IsNullOrEmpty(query.ResultsTransformer) == false)
            {
                var transformGenerator = database.IndexDefinitionStorage.GetTransformer(query.ResultsTransformer);

                if (transformGenerator != null && transformGenerator.TransformResultsDefinition != null)
                    transformFunc = transformGenerator.TransformResultsDefinition;
                else
                    throw new InvalidOperationException("The transformer " + query.ResultsTransformer + " was not found");
            }

            IEnumerable<RavenJObject> results;
            var transformerErrors = new List<string>();

            if (transformFunc == null)
                results = documents.Select(x => x.ToJson());
            else
            {
                var robustEnumerator = new RobustEnumerator(token, 100,
                    onError: (exception, o) => transformerErrors.Add(string.Format("Doc '{0}', Error: {1}", Index.TryGetDocKey(o), exception.Message)));

                results = robustEnumerator
                    .RobustEnumeration(documents.Select(x => new DynamicJsonObject(x.ToJson())).GetEnumerator(), transformFunc)
                    .Select(JsonExtensions.ToJObject);
            }

            return results;
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
                    .Select(docId => database.Documents.Get(docId, null))
                    .Where(it => it != null)
                    .ToArray();
            }

            var fields = searcher.Doc(baseDocId).GetFields().Cast<AbstractField>().Select(x => x.Name).Distinct().ToArray();
            var etag = database.Indexes.GetIndexEtag(indexName, null);
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

        private JsonDocument[] GetJsonDocuments(IndexSearcher searcher, Index index, string indexName, ScoreDoc[] scoreDocs)
        {
            if (scoreDocs.Any())
            {
                // Since we don't have a document we get the fields from the first hit
                var fields = searcher.Doc(scoreDocs.First().Doc).GetFields().Cast<AbstractField>().Select(x => x.Name).Distinct().ToArray();
                var etag = database.Indexes.GetIndexEtag(indexName, null);
                return scoreDocs
                    .Select(hit => new JsonDocument
                    {
                        DataAsJson = Index.CreateDocumentFromFields(searcher.Doc(hit.Doc),
                            new FieldsToFetch(fields, false, index.IsMapReduce ? Constants.ReduceKeyFieldName : Constants.DocumentIdFieldName)),
                        Etag = etag
                    })
                    .ToArray();
            }

            return new JsonDocument[0];
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

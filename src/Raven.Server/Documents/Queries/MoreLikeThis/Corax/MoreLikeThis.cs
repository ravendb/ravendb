using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Analyzers;
using Corax.Querying.Matches.Meta;
using Lucene.Net.Util;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using CoraxProj = global::Corax;

/*
 * This code is adaptation of `MoreLikeThis` functionality from Lucene for Corax. Original code is shared on licence:
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

  You can find original source-code here: https://github.com/ravendb/ravendb/blob/v5.3/src/Raven.Server/Documents/Queries/MoreLikeThis/MoreLikeThis.cs
*/

namespace Raven.Server.Documents.Queries.MoreLikeThis.Corax;

internal class RavenMoreLikeThis : MoreLikeThisBase, IDisposable
{
    private readonly CoraxQueryBuilder.Parameters _builderParameters;
    private readonly Analyzer _analyzer;
    private readonly AnalyzersScope _analyzersScope;

    public RavenMoreLikeThis(CoraxQueryBuilder.Parameters builderParameters, Analyzer analyzer = null)
    {
        _analyzer = analyzer ?? Analyzer.CreateDefaultAnalyzer(builderParameters.Allocator);
        _builderParameters = builderParameters;
        _analyzersScope = new(builderParameters.IndexSearcher, builderParameters.IndexFieldsMapping, builderParameters.HasDynamics);
    }

    protected override PriorityQueue<object[]> CreateQueue(Dictionary<string, int> words)
    {
        var indexSearcher = _builderParameters.IndexSearcher;
        var amountOfDocs = indexSearcher.NumberOfEntries;
        var res = new FreqQ(words.Count);

        foreach (var (word, tf) in words.GetEnumerator())
        {
            if (_minTermFreq > 0 && tf < _minTermFreq)
            {
                continue;
            }

            var topField = _fieldNames[0];
            var docFreq = 0L;

            foreach (var fieldName in _fieldNames)
            {
                var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(_builderParameters.Allocator, fieldName, _builderParameters.Index, _builderParameters.IndexFieldsMapping, _builderParameters.FieldsToFetch, _builderParameters.HasDynamics, _builderParameters.DynamicFields, handleSearch: true);
                var freq = indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMetadata, word);
                topField = freq > docFreq ? fieldName : topField;
                docFreq = freq > docFreq ? freq : docFreq;
            }

            if (_minDocFreq > 0 && docFreq < _minDocFreq)
            {
                continue; // filter out words that don't occur in enough docs
            }

            if (docFreq > _maxDocfreq)
            {
                continue; // filter out words that occur in too many docs
            }

            if (docFreq == 0)
            {
                continue; // index update problem?
            }

            var idf = Idf(docFreq, amountOfDocs);
            var score = tf * idf;

            // only really need 1st 3 entries, other ones are for troubleshooting
            res.InsertWithOverflow(new object[] {word, topField, score, idf, docFreq, tf});
        }

        return res;
    }

    //https://en.wikipedia.org/wiki/Tf%E2%80%93idf#Inverse_document_frequency_2
    private float Idf(long docFreq, long totalAmountOfDocs) => (float)totalAmountOfDocs / docFreq;

    protected override void AddTermFrequencies(TextReader r, Dictionary<string, int> termFreqMap, string fieldName)
    {
        //We dont have any streaming option for analyzing in Corax so we've to read all
        var termOriginal = Encoding.UTF8.GetBytes(r.ReadToEnd());

        using var _ = Slice.From(this._builderParameters.Allocator, fieldName, ByteStringType.Immutable, out var fieldNameSlice);
        using var __ = _analyzersScope.Execute(_analyzer, termOriginal, out var outputBuffer, out var outputTokens);

        var tokenCount = 0;
        foreach (var token in outputTokens)
        {
            var word = Encoding.UTF8.GetString(outputBuffer.Slice(token.Offset, (int)token.Length));
            tokenCount++;

            if (tokenCount > _maxNumTokensParsed)
            {
                break;
            }

            if (IsNoiseWord(word))
            {
                continue;
            }

            // increment frequency
            ref var tf = ref CollectionsMarshal.GetValueRefOrAddDefault(termFreqMap, word, out var _);
            tf++;
        }
    }
    
    public IQueryMatch Like(long documentId)
    {
        if (_fieldNames == null) throw new InvalidDataException($"FieldNames are empty!");

        return CreateQuery(RetrieveTerms(documentId));
    }

    internal IQueryMatch Like(BlittableJsonReaderObject json)
    {
        return CreateQuery(RetrieveTerms(json));
    }

    IQueryMatch CreateQuery(PriorityQueue<object[]> q)
    {
        var indexSearcher = _builderParameters.IndexSearcher;
        object cur;
        var qterms = 0;
        IQueryMatch query = null;
        while ((cur = q.Pop()) is not null)
        {
            var ar = cur as object[];

            Debug.Assert(ar is not null);
            var fieldName = ar[1] as string;

            Debug.Assert(fieldName is not null);
            var term = ar[0] as string;

            if (_boost)
            {
                throw new NotSupportedInCoraxException("Boosting inside MoreLikeThis is not supported yet.");
            }

            var fieldMetadata = indexSearcher.GetFieldMetadata(fieldName);
            query = query is null 
                ? indexSearcher.TermQuery(fieldMetadata, term) 
                : indexSearcher.Or(query, indexSearcher.TermQuery(fieldMetadata, term));
            
            qterms++;

            if (_maxQueryTerms > 0 && qterms >= _maxQueryTerms)
                break;
        }

        return query ?? indexSearcher.EmptyMatch();
    }

    /// <summary>
    /// Set the maximum percentage in which words may still appear. Words that appear
    /// in more than this many percent of all docs will be ignored.
    /// </summary>
    /// <param name="maxPercentage">
    /// the maximum percentage of documents (0-100) that a term may appear 
    /// in to be still considered relevant
    /// </param>
    public override void SetMaxDocFreqPct(int maxPercentage)
    {
        var result = checked((maxPercentage / 100.0) * _builderParameters.IndexSearcher.NumberOfEntries);
        _maxDocfreq = result >= int.MaxValue ? int.MaxValue : (int)Math.Ceiling(result);
    }

    protected PriorityQueue<object[]> RetrieveTerms(long documentId)
    {
        var indexSearcher = _builderParameters.IndexSearcher;
        Dictionary<string, int> termFreqMap = new();
        Page p = default;
        
        using var _ = indexSearcher.GetEntryTermsReader(documentId, ref p, out var indexEntry);

        var fields = _fieldNames.Select(x => indexSearcher.FieldCache.GetLookupRootPage(x)).ToHashSet();

        while (indexEntry.MoveNext())
        {
            if (fields.Contains(indexEntry.FieldRootPage) == false)
                continue;

            if (indexEntry.IsNonExisting)
                continue;


            var key = indexEntry.IsNull
                ? null
                : indexEntry.Current.Decoded();
            InsertTerm(key, indexEntry.Frequency);
        }

        return CreateQueue(termFreqMap);
        
        
        void InsertTerm(ReadOnlySpan<byte> term, int freq)
        {
            var termAsString = term is {Length: 0} 
                ? CoraxProj.Constants.ProjectionNullValue 
                : Encoding.UTF8.GetString(term);
            
            if (IsNoiseWord(termAsString)) // TODO optimize
                return;

            ref var counter = ref CollectionsMarshal.GetValueRefOrAddDefault(termFreqMap, termAsString, out var __);
            counter += freq;
        }
    }

    public void Dispose()
    {
        _analyzersScope?.Dispose();
    }
}

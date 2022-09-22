using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Corax;
using Corax.Pipeline;
using Corax.Queries;
using Lucene.Net.Support;
using Lucene.Net.Util;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Sparrow.Json;
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

internal class RavenMoreLikeThis : MoreLikeThisBase
{
    private readonly CoraxQueryBuilder.Parameters _builderParameters;
    private readonly Analyzer _analyzer;


    public RavenMoreLikeThis(CoraxQueryBuilder.Parameters builderParameters, Analyzer analyzer = null)
    {
        _analyzer = analyzer ?? Analyzer.DefaultAnalyzer;
        _builderParameters = builderParameters;
    }

    protected override PriorityQueue<object[]> CreateQueue(IDictionary<string, Int> words)
    {
        var indexSearcher = _builderParameters.IndexSearcher;
        var amountOfDocs = indexSearcher.NumberOfEntries;
        var res = new FreqQ(words.Count);

        foreach (var (word, value) in words.GetEnumerator())
        {
            var tf = value.X;

            if (_minTermFreq > 0 && tf < _minTermFreq)
            {
                continue;
            }

            var topField = _fieldNames[0];
            var docFreq = 0L;

            foreach (var fieldName in _fieldNames)
            {
                var fieldId = QueryBuilderHelper.GetFieldId(fieldName, _builderParameters.Index, _builderParameters.IndexFieldsMapping, _builderParameters.FieldsToFetch);
                var freq = indexSearcher.TermAmount(fieldName, word, fieldId);
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

    protected override void AddTermFrequencies(TextReader r, IDictionary<string, Int> termFreqMap, string fieldName)
    {
        //We dont have any streaming option for analyzing in Corax so we've to read all
        var termOriginal = Encoding.UTF8.GetBytes(r.ReadToEnd());
        var tokenCount = 0;
        _analyzer.GetOutputBuffersSize(termOriginal.Length, out int outputSize, out int tokenSize);
        var bufferHandler = Analyzer.BufferPool.Rent(outputSize);
        var tokensHandler = Analyzer.TokensPool.Rent(tokenSize);

        var buffer = bufferHandler.AsSpan();
        var tokens = tokensHandler.AsSpan();
        _analyzer.Execute(termOriginal, ref buffer, ref tokens);

        foreach (var token in tokens)
        {
            var word = Encoding.UTF8.GetString(buffer.Slice(token.Offset, (int)token.Length));
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
            var cnt = termFreqMap[word];
            if (cnt == null)
            {
                termFreqMap[word] = new Int();
            }
            else
            {
                cnt.X++;
            }
        }

        Analyzer.BufferPool.Return(bufferHandler);
        Analyzer.TokensPool.Return(tokensHandler);
    }

    /// <summary> Adds term frequencies found by tokenizing text from reader into the Map words</summary>
    protected void AddTermFrequencies(ref IndexEntryReader entryReader, IDictionary<string, Int> termFreqMap, string fieldName)
    {
        if (_builderParameters.IndexFieldsMapping.TryGetByFieldName(fieldName, out var binding) == false || binding.FieldIndexingMode is FieldIndexingMode.No)
        {
            //We don't have such data in index so nothing to do
            return;
        }

        var analyzer = binding.Analyzer;

        byte[] buffer = Array.Empty<byte>();
        Token[] tokens = Array.Empty<Token>();
        var fieldType = entryReader.GetFieldType(binding.FieldId, out _); // var type, out Span<byte> sourceTerm) == false)
        switch (fieldType)
        {
            case IndexEntryFieldType.Empty:
            case IndexEntryFieldType.Null:
                var termValue = fieldType == IndexEntryFieldType.Null ? CoraxProj.Constants.NullValueSlice : CoraxProj.Constants.EmptyStringSlice;
                InsertTerm(termValue.AsReadOnlySpan());
                break;

            case IndexEntryFieldType.TupleListWithNulls:
            case IndexEntryFieldType.TupleList:
                if (entryReader.TryReadMany(binding.FieldId, out var iterator) == false)
                    break;

                while (iterator.ReadNext())
                {
                    if (iterator.IsNull)
                    {
                        InsertTerm(CoraxProj.Constants.NullValueSlice);
                    }
                    else if (iterator.IsEmpty)
                    {
                        throw new InvalidDataException("Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)");
                    }
                    else
                    {
                        InsertTerm(iterator.Sequence);
                    }
                }

                break;

            case IndexEntryFieldType.Tuple:
                if (entryReader.Read(binding.FieldId, out _, out long lVal, out double dVal, out Span<byte> valueInEntry) == false)
                    break;

                InsertTerm(valueInEntry);
                break;

            case IndexEntryFieldType.SpatialPointList:
                if (entryReader.TryReadManySpatialPoint(binding.FieldId, out var spatialIterator) == false)
                    break;

                while (spatialIterator.ReadNext())
                {
                    for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                        InsertTerm(spatialIterator.Geohash.Slice(0, i));
                }

                break;

            case IndexEntryFieldType.SpatialPoint:
                if (entryReader.Read(binding.FieldId, out valueInEntry) == false)
                    break;

                for (int i = 1; i <= valueInEntry.Length; ++i)
                    InsertTerm(valueInEntry.Slice(0, i));

                break;

            case IndexEntryFieldType.ListWithNulls:
            case IndexEntryFieldType.List:
                if (entryReader.TryReadMany(binding.FieldId, out iterator) == false)
                    break;

                while (iterator.ReadNext())
                {
                    Debug.Assert((fieldType & IndexEntryFieldType.Tuple) == 0, "(fieldType & IndexEntryFieldType.Tuple) == 0");

                    if ((fieldType & IndexEntryFieldType.HasNulls) != 0 && (iterator.IsEmpty || iterator.IsNull))
                    {
                        var fieldValue = iterator.IsNull ? CoraxProj.Constants.NullValueSlice : CoraxProj.Constants.EmptyStringSlice;
                        InsertTerm(fieldValue.AsReadOnlySpan());
                    }
                    else
                    {
                        InsertTerm(iterator.Sequence);
                    }
                }

                break;
            case IndexEntryFieldType.Raw:
            case IndexEntryFieldType.RawList:
            case IndexEntryFieldType.Invalid:
                break;
            default:
                if (entryReader.Read(binding.FieldId, out var value) == false)
                    break;

                InsertTerm(value);
                break;
        }

        Analyzer.BufferPool.Return(buffer);
        Analyzer.TokensPool.Return(tokens);

        void InsertTerm(ReadOnlySpan<byte> termToAdd)
        {
            UnlikelyGrowBuffer(termToAdd);
            var bufferSpan = buffer.AsSpan();
            var tokensSpan = tokens.AsSpan();
            analyzer.Execute(termToAdd, ref bufferSpan, ref tokensSpan);
            for (int index = 0; index < tokensSpan.Length; index++)
            {
                Token token = tokensSpan[index];
                var term = bufferSpan.Slice(token.Offset, (int)token.Length);
                if (index > _maxNumTokensParsed)
                    break;

                var termAsString = Encoding.UTF8.GetString(term);
                if (IsNoiseWord(termAsString)) // TODO optimize
                    continue;

                var counter = termFreqMap[termAsString];
                if (counter is null)
                    termFreqMap[termAsString] = new();
                else
                    counter.X++;
            }
        }

        void UnlikelyGrowBuffer(ReadOnlySpan<byte> input)
        {
            analyzer.GetOutputBuffersSize(input.Length, out var outputSize, out int tokenSize);
            if (tokenSize > tokens.Length)
            {
                Analyzer.TokensPool.Return(tokens);
                tokens = null;
                tokens = Analyzer.TokensPool.Rent(tokenSize);
            }

            if (outputSize > buffer.Length)
            {
                Analyzer.BufferPool.Return(buffer);
                buffer = null;
                buffer = Analyzer.BufferPool.Rent(outputSize);
            }
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

            query = query is null ? indexSearcher.TermQuery(fieldName, term) : indexSearcher.Or(query, indexSearcher.TermQuery(fieldName, term));
            qterms++;

            if (_maxQueryTerms > 0 && qterms >= _maxQueryTerms)
                break;
        }

        return query ?? indexSearcher.EmptySet();
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
        IDictionary<string, Int> termFreqMap = new HashMap<string, Int>();
        var indexEntry = indexSearcher.GetReaderFor(documentId);

        for (var i = 0; i < _fieldNames.Length; i++)
        {
            var fieldName = _fieldNames[i];
            AddTermFrequencies(ref indexEntry, termFreqMap, fieldName);
        }

        return CreateQueue(termFreqMap);
    }
}

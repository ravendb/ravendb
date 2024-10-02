using System;
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Raven.Client.Documents.Queries.MoreLikeThis;

namespace Raven.Server.Documents.Queries.MoreLikeThis.Lucene;

internal sealed class RavenMoreLikeThis : LuceneMoreLikeThis
{
    private readonly IndexReader _ir;
    private readonly IState _state;

    public RavenMoreLikeThis(IndexReader ir, MoreLikeThisOptions options, IState state)
        : base(ir, state)
    {
        _ir = ir;
        _state = state;

        if (options.Boost != null)
            Boost = options.Boost.Value;
        if (options.BoostFactor != null)
            BoostFactor = options.BoostFactor.Value;
        if (options.MaximumNumberOfTokensParsed != null)
            MaxNumTokensParsed = options.MaximumNumberOfTokensParsed.Value;
        if (options.MaximumQueryTerms != null)
            MaxQueryTerms = options.MaximumQueryTerms.Value;
        if (options.MinimumWordLength != null)
            MinWordLen = options.MinimumWordLength.Value;
        if (options.MaximumWordLength != null)
            MaxWordLen = options.MaximumWordLength.Value;
        if (options.MinimumTermFrequency != null)
            MinTermFreq = options.MinimumTermFrequency.Value;
        if (options.MinimumDocumentFrequency != null)
            MinDocFreq = options.MinimumDocumentFrequency.Value;
        if (options.MaximumDocumentFrequency != null)
            MaxDocFreq = options.MaximumDocumentFrequency.Value;
        if (options.MaximumDocumentFrequencyPercentage != null)
            SetMaxDocFreqPct(options.MaximumDocumentFrequencyPercentage.Value);
    }

    protected override PriorityQueue<object[]> RetrieveTerms(int docNum)
    {
        var fieldNames = GetFieldNames() ?? Array.Empty<string>();

        Dictionary<string, int> termFreqMap = new();

        foreach (var fieldName in fieldNames)
        {
            var vector = _ir.GetTermFreqVector(docNum, fieldName, _state);

            // field does not store term vector info
            if (vector == null)
            {
                var d = _ir.Document(docNum, _state);
                var text = d.GetValues(fieldName, _state);
                if (text != null)
                {
                    foreach (var t in text)
                    {
                        AddTermFrequencies(new StringReader(t), termFreqMap, fieldName);
                    }
                }
            }
            else
            {
                AddTermFrequencies(termFreqMap, vector);
            }
        }

        return CreateQueue(termFreqMap);
    }
}

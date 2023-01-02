using System;

namespace Corax.Utils;

// Highly inspired by: https://www.researchgate.net/publication/45886647_Integrating_the_Probabilistic_Models_BM25BM25F_into_Lucene

public class Bm25
{
    public const float BFactor = 0.25f;
    public const float Bias = 0.75f;
    public const float K1 = 2f;
    public const float LcAvlC = .7f; //todo
    

    public static float ComputeIdf(IndexSearcher indexSearcher, long termFrequency)
    {
        var m = indexSearcher.NumberOfEntries - termFrequency + 0.5D;
        var d = termFrequency + 0.5D;

        return (float)Math.Log(m / d);
    }

    public static void CalculateRelevance(in FrequencyHolder items, float idf, Span<long> matches, Span<float> scores)
    {
        var innerItems = items.Matches;
        var frequencies = items.Scores;
        
        for (int idX = 0; idX < matches.Length; ++idX)
        {
            var entryId = matches[idX];
            var idOfInner = innerItems.BinarySearch(entryId);
            
            if (idOfInner < 0)
                continue;

            var weight = frequencies[idOfInner] / ((1 - BFactor) + BFactor * LcAvlC);
            scores[idX] += idf * weight / (K1 + weight);
        }
    }
    
    
    //public static float GetWeight(long occurance, )
    // private IndexSearcher _searcher;
    // private float? _documentLenFactor;
    // public float DocumentLenFactor => _documentLenFactor ??= CalculateDocumentLenFactor();
    //
    // private float? CalculateDocumentLenFactor()
    // {
    //     if (_searcher.d)
    // }
    //
    // public Bm25(IndexSearcher searcher)
    // {
    //     
    // }
    //
    // private long? Av
}

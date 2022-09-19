using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Indexes.Persistence.Corax;

namespace Raven.Server.Documents.Queries.MoreLikeThis.Corax;

internal sealed class RavenRavenMoreLikeThis : RavenMoreLikeThis
{
    public RavenRavenMoreLikeThis(QueryParameters env, MoreLikeThisOptions options) : base(env)
    {
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
            base.SetMaxDocFreqPct(options.MaximumDocumentFrequencyPercentage.Value);
    }
}

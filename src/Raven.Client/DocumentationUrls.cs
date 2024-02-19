namespace Raven.Client;

internal static class DocumentationUrls
{
    internal static class Session
    {
        internal static class Querying
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/document-query/what-is-document-query"/></remarks>
            public const string WhatIsDocumentQuery = nameof(WhatIsDocumentQuery);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/debugging/include-explanations"/></remarks>
            public const string IncludeExplanations = nameof(IncludeExplanations);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/text-search/highlight-query-results"/></remarks>
            public const string HighlightQueryResults = nameof(HighlightQueryResults);

            internal static class HowToCustomizeQuery
            {
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/how-to-customize-query#beforequeryexecuted"/></remarks>
                public const string BeforeQueryExecuted = nameof(BeforeQueryExecuted);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/how-to-customize-query#afterqueryexecuted"/></remarks>
                public const string AfterQueryExecuted = nameof(AfterQueryExecuted);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/how-to-customize-query#afterstreamexecuted"/></remarks>
                public const string AfterStreamExecuted = nameof(AfterStreamExecuted);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/how-to-customize-query#nocaching"/></remarks>
                public const string NoCaching = nameof(NoCaching);

                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/how-to-customize-query#notracking"/></remarks>
                public const string NoTracking = nameof(NoTracking);
            }

            internal static class SortQueryResults
            {
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/sort-query-results#order-by-random"/></remarks>
                public const string OrderByRandom = nameof(OrderByRandom);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/sort-query-results#custom-sorters"/></remarks>
                public const string CustomSorters = nameof(CustomSorters);
            }

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/indexes/querying/faceted-search"/></remarks>
            public const string AggregationQuery = nameof(AggregationQuery);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/indexes/querying/faceted-search#storing-facets-definition-in-a-document"/></remarks>
            public const string AggregationQuerySetup = nameof(AggregationQuerySetup);
            
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/Csharp/client-api/session/querying/how-to-use-morelikethis"/></remarks>
            public const string MoreLikeThisQuery = nameof(MoreLikeThisQuery);
            
            ///<remarks><seealso ref="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/how-to-work-with-suggestions"/></remarks>
            public const string SuggestionsQuery = nameof(SuggestionsQuery);
        }

        internal static class Transactions
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/faq/transaction-support"/></remarks>
            public const string TransactionSupport = nameof(TransactionSupport);
        }

        internal static class Options
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/configuration/how-to-disable-tracking#disable-entity-tracking"/></remarks>
            public const string NoTracking = nameof(NoTracking);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/configuration/how-to-disable-caching"/></remarks>
            public const string NoCaching = nameof(NoCaching);
        }

        internal static class Sharding
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/sharding/administration/anchoring-documents#sharding-anchoring-documents"/></remarks>
            public const string Anchoring = nameof(Anchoring);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/sharding/querying#querying-a-selected-shard"/></remarks>
            public const string QueryingASelectedShard = nameof(QueryingASelectedShard);
        }
    }
}

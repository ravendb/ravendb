namespace Raven.Client;

internal static class DocumentationUrls
{
    internal static class Session
    {
        internal static class Querying
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/document-query/what-is-document-query"/></remarks>
            public const string WhatIsDocumentQuery = nameof(WhatIsDocumentQuery);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/debugging/include-explanations"/></remarks>
            public const string IncludeExplanations = nameof(IncludeExplanations);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/text-search/highlight-query-results"/></remarks>
            public const string HighlightQueryResults = nameof(HighlightQueryResults);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/Csharp/client-api/session/querying/debugging/query-timings"/></remarks>
            public const string QueryTimings = nameof(QueryTimings);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-query#session.advanced.rawquery"/></remarks>
            public const string RawDocumentQuery = nameof(RawDocumentQuery);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/indexes/querying/projections#projection-behavior-with-a-static-index"/></remarks>
            public const string ProjectionBehavior = nameof(ProjectionBehavior);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/indexes/querying/faceted-search"/></remarks>
            public const string FacetedSearch = nameof(FacetedSearch);

            internal static class HowToCustomizeQuery
            {
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-customize-query#beforequeryexecuted"/></remarks>
                public const string BeforeQueryExecuted = nameof(BeforeQueryExecuted);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-customize-query#afterqueryexecuted"/></remarks>
                public const string AfterQueryExecuted = nameof(AfterQueryExecuted);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-customize-query#afterstreamexecuted"/></remarks>
                public const string AfterStreamExecuted = nameof(AfterStreamExecuted);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-customize-query#nocaching"/></remarks>
                public const string NoCaching = nameof(NoCaching);

                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-customize-query#notracking"/></remarks>
                public const string NoTracking = nameof(NoTracking);
            }

            internal static class SortQueryResults
            {
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/sort-query-results#order-by-field-value"/></remarks>
                public const string OrderByFieldValue = nameof(OrderByFieldValue);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/sort-query-results#order-by-random"/></remarks>
                public const string OrderByRandom = nameof(OrderByRandom);

                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/sort-query-results#order-by-score"/></remarks>
                public const string OrderByScore = nameof(OrderByScore);
                
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/sort-query-results#custom-sorters"/></remarks>
                public const string CustomSorters = nameof(CustomSorters);
            }

            internal static class StreamQueryResults
            {
                /// <remarks>See <seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-stream-query-results#stream-by-query">
                /// How to stream query results
                /// </seealso></remarks>
                public const string ByQuery = nameof(ByQuery);

                /// <remarks>See <seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-stream-query-results#stream-results-by-prefix">
                /// How to stream query results by prefix
                /// </seealso></remarks>
                public const string ByPrefix = nameof(ByPrefix);
            }

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/indexes/querying/faceted-search"/></remarks>
            public const string AggregationQuery = nameof(AggregationQuery);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/indexes/querying/faceted-search#storing-facets-definition-in-a-document"/></remarks>
            public const string AggregationQuerySetup = nameof(AggregationQuerySetup);
            
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/Csharp/client-api/session/querying/how-to-use-morelikethis"/></remarks>
            public const string MoreLikeThisQuery = nameof(MoreLikeThisQuery);
            
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-work-with-suggestions"/></remarks>
            public const string SuggestionsQuery = nameof(SuggestionsQuery);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-use-intersect"/></remarks>
            public const string HowToUseIntersect = nameof(HowToUseIntersect);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/text-search/fuzzy-search"/></remarks>
            public const string FuzzySearch = nameof(FuzzySearch);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/text-search/boost-search-results"/></remarks>
            public const string BoostSearchResults = nameof(BoostSearchResults);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/text-search/proximity-search"/></remarks>
            public const string ProximitySearch = nameof(ProximitySearch);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-make-a-spatial-query"/></remarks>
            public const string HowToMakeASpatialQuery = nameof(HowToMakeASpatialQuery);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/document-query/query-vs-document-query"/></remarks>
            public const string QueryVsDocumentQuery = nameof(QueryVsDocumentQuery);
            
            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/indexes/querying/distinct"/></remarks>
            public const string Distinct = nameof(Distinct);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/Csharp/client-api/session/querying/how-to-perform-group-by-query"/></remarks>
            public const string GroupByQuery = nameof(GroupByQuery);

            /// <remarks>
            /// <seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-perform-group-by-query#group-by-array"/> <br/>
            /// <seealso href="https://ravendb.net/docs/article-page/7.0/Csharp/indexes/indexing-nested-data" />
            /// </remarks>
            public const string GroupByArrayQuery = nameof(GroupByArrayQuery);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/how-to-perform-group-by-query#by-array-content"/></remarks>
            public const string GroupByArrayContent = nameof(GroupByArrayContent);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/document-query/how-to-use-not-operator"/></remarks>
            public const string HowToUseNotOperator = nameof(HowToUseNotOperator);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/querying/document-query/how-to-use-lucene"/></remarks>
            public const string HowToUseLucene = nameof(HowToUseLucene);

            /// <remarks><see href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/how-to/handle-document-relationships#includes"/></remarks>
            public const string Includes = nameof(Includes);

        }

        internal static class Counters
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/counters/overview"/></remarks>
            public const string Overview = nameof(Overview);
        }

        internal static class Transactions
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/faq/transaction-support"/></remarks>
            public const string TransactionSupport = nameof(TransactionSupport);
        }

        internal static class Options
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/configuration/how-to-disable-tracking#disable-entity-tracking"/></remarks>
            public const string NoTracking = nameof(NoTracking);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/session/configuration/how-to-disable-caching"/></remarks>
            public const string NoCaching = nameof(NoCaching);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/configuration/conventions"/></remarks>
            public const string Conventions = nameof(Conventions);
        }

        internal static class Sharding
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/sharding/administration/anchoring-documents#sharding-anchoring-documents"/></remarks>
            public const string Anchoring = nameof(Anchoring);

            /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/sharding/querying#querying-a-selected-shard"/></remarks>
            public const string QueryingASelectedShard = nameof(QueryingASelectedShard);
        }

        internal static class HowTo
        {
            internal static class HandleDocumentRelationships
            {
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/client-api/how-to/handle-document-relationships#includes"/></remarks>
                public const string Includes = nameof(Includes);
            }
        }

        internal static class TimeSeries
        {
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/client-api/session/get/get-entries#include-parent-and-tagged-documents"/></remarks>
            public const string Include = nameof(Include);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/client-api/session/include/with-session-query"/></remarks>
            public const string IncludeWithQuery = nameof(IncludeWithQuery);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/client-api/overview"/></remarks>
            public const string ClientApi = nameof(ClientApi);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/client-api/session/get/get-entries#timeseriesfor.get"/></remarks>
            public const string GetOperation = nameof(GetOperation);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/client-api/session/append"/></remarks>
            public const string AppendOperation = nameof(AppendOperation);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/client-api/session/delete"/></remarks>
            public const string DeleteOperation = nameof(DeleteOperation);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/incremental-time-series/client-api/session/increment"/></remarks>
            public const string IncrementOperation = nameof(IncrementOperation);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/incremental-time-series/overview"/></remarks>
            public const string IncrementalOverview = nameof(IncrementalOverview);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/client-api/named-time-series-values"/></remarks>
            public const string NamedValues = nameof(NamedValues);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/7.0/csharp/document-extensions/timeseries/rollup-and-retention"/></remarks>
            public const string RollupAndRetention = nameof(RollupAndRetention);
        }
    }

    internal static class Operations
    {
        internal static class ServerOperations
        {
            internal static class Analyzers
            {
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/Csharp/indexes/using-analyzers#creating-custom-analyzers"/></remarks>
                public static string CustomAnalyzers => nameof(CustomAnalyzers);
            }
            
            internal static class Sorters
            {
                /// <remarks><seealso href="https://ravendb.net/docs/article-page/6.0/Csharp/client-api/operations/maintenance/sorters/put-sorter#put-custom-sorter"/></remarks>
                public static string CustomSorters = nameof(CustomSorters);
            }

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/operations/maintenance/clean-change-vector"/></remarks>
            public static string UpdateUnusedDatabasesOperation = nameof(UpdateUnusedDatabasesOperation);
            
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/Csharp/client-api/operations/maintenance/indexes/set-index-lock"/></remarks>
            public static string SetDatabasesLockOperation = nameof(SetDatabasesLockOperation);  
            
            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/Csharp/client-api/operations/server-wide/toggle-dynamic-database-distribution"/></remarks>
            public static string SetDatabaseDynamicDistributionOperation = nameof(SetDatabaseDynamicDistributionOperation);
            
            
        }
    }
}

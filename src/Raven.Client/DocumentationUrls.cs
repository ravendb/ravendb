namespace Raven.Client;

internal static class DocumentationUrls
{
    internal static class Session
    {
        internal static class Querying
        {
            ///<remarks><seealso ref="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/document-query/what-is-document-query"/></remarks>
            public const string WhatIsDocumentQuery = nameof(WhatIsDocumentQuery);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/debugging/include-explanations"/></remarks>
            public const string IncludeExplanations = nameof(IncludeExplanations);

            ///<remarks><seealso ref="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/text-search/highlight-query-results"/></remarks>
            public const string HighlightQueryResults = nameof(HighlightQueryResults);
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
        }
    }
}

namespace Raven.Client.Documents
{
    public enum EscapeQueryOptions
    {
        EscapeAll,
        AllowPostfixWildcard,
        /// <summary>
        /// This allows queries such as Name:*term*, which tend to be much
        /// more expensive and less performant than any other queries. 
        /// Consider carefully whether you really need this, as there are other
        /// alternative for searching without doing extremely expensive leading 
        /// wildcard matches.
        /// </summary>
        AllowAllWildcards,
        RawQuery
    }
}
using System;

namespace Raven.Server.Documents.Queries;

/// <summary>
/// Captures DateTime.UtcNow once, ensuring all now()/today() calls
/// within the same query execution resolve to the same timestamp.
/// Pass this instance through the query building and ETag computation chain.
/// </summary>
public sealed class QueryTimeScope
{
    public readonly DateTime Now;
    public readonly DateTime Today;

    public QueryTimeScope()
    {
        Now = DateTime.UtcNow;
        Today = Now.Date;
    }
}

using System.Collections.Generic;
using PgSqlParser;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    // Data records produced by PowerBI recognition (PowerBIWrapperRecognizer / PowerBIShapeClassifier)
    // and consumed by the rewriters in PowerBIDirectQuery.

    internal sealed record DirectQueryShape(
        List<string> ProjectionCols,
        int Limit);

    internal sealed record Aggregate(
        string FunctionName,
        string FieldName,
        string OutputColumn);

    internal sealed record GroupedAggregateShape(
        List<string> GroupByFields,
        List<Aggregate> Aggregates,
        List<string> OrderByCols,
        List<bool> OrderByDescFlags,
        int Limit);

    internal enum GroupedOrderByKind
    {
        Output,
        GroupKey
    }

    internal sealed record GroupedAggregateOrderByPart(
        GroupedOrderByKind Kind,
        int? GroupKeyIndex,
        int? AggregateIndex,
        bool Desc);

    internal sealed record GroupedAggregateRqlParts(
        string FromText,
        string WhereText,
        List<string> GroupByFields,
        List<Aggregate> Aggregates,
        List<GroupedAggregateOrderByPart> OrderBy,
        int Limit);

    internal sealed record NormalizedWrapper(
        List<string> OuterProjectedColumns,
        List<string> GroupByColumns,
        List<string> OrderByColumns,
        List<bool> OrderByDescFlags,
        Node OuterWhereClause,
        int? Limit,
        int? Offset,
        List<Aggregate> Aggregates,
        // WHEREs found at intermediate wrapper levels (not the outermost SELECT). PowerBI's DirectQuery
        // can plant user filters several wrapper levels deep — e.g. inside the level that does the
        // distinct-grouping that precedes the null-ordering CASE helpers. Each entry carries the raw
        // WHERE AST node plus the wrapper alias used in that level's SELECT, so the WHERE translator
        // knows which alias the column refs are anchored to when collapsing them onto the inner RQL.
        List<IntermediateWhere> IntermediateWheres);

    internal sealed record IntermediateWhere(
        Node WhereClause,
        string WrapperAlias);
}

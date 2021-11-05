using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public static class PowerBIFetchQuery
    {
        /// <summary>
        /// Match an SQL from PowerBI that intends to query a collection. The SQL query may be nested and 
        /// may also have a nested RQL query.
        /// </summary>
        private static readonly Regex FetchSqlRegex = new(@"(?is)^\s*(?:select\s+(?:\*|(?:(?:(?:""(\$Table|_)""\.)?""(?<src_columns>[^""]+)""(?:\s+as\s+""(?<all_columns>(?<dest_columns>[^""]+))"")?(?<replace>)|(?<replace>replace)\(""_"".""(?<src_columns>[^""]+)"",\s+'(?<replace_inputs>[^']*)',\s+'(?<replace_texts>[^']*)'\)\s+as\s+""(?<all_columns>(?<dest_columns>[^""]+))"")(?:\s|,)*)+)\s+from\s+(?:(?:\((?:\s|,)*)(?<inner_query>.*)\s*\)|""public"".""(?<table_name>.+)""))\s+""(?:\$Table|_)""(\s+where\s+(?<where>.*?))?(?:\s+limit\s+(?<limit>[0-9]+))?\s*$",
            RegexOptions.Compiled);

        /// <summary>
        /// Match the column names found in the SQL where clause. 
        /// Used to integrate the column names into the where clause of the RQL query.
        /// </summary>
        private static readonly Regex WhereColumnRegex = new(@"""_""\.""(?<column>.*?)""", RegexOptions.Compiled);

        /// <summary>
        /// Match operators found in the SQL where clause. 
        /// Used to integrate the where clause into the RQL query.
        /// </summary>
        private static readonly Regex WhereOperatorRegex = new(@"(?=.*?\s+)is(\s+not)?(?=\s+.+?)", RegexOptions.Compiled);

        /// <summary>
        /// Map of operators from PostgreSQL to RQL
        /// </summary>
        private static readonly Dictionary<string, string> OperatorMap = new(StringComparer.OrdinalIgnoreCase)
        {
            { "is", "=" },
            { "is not", "!=" },
        };

        private static readonly Regex TimestampConditionRegex = new(@"timestamp\ \'(?<date>.*?)\'", RegexOptions.Compiled);

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            // Match queries sent by PowerBI, either RQL queries wrapped in an SQL statement OR generic SQL queries
            if (TryGetMatches(queryText, out var matches, out var rql) == false)
            {
                pgQuery = null;
                return false;
            }

            Dictionary<string, ReplaceColumnValue> powerBiReplaceValues = GetReplaceValues(matches);

            string newRql = null;

            if (rql != null)
            {
                // RQL query coming  from 'SQL statement (optional, requires database)' text box in Power BI

                var powerBiFiltering = GetSqlWhereConditions(matches, rql.From.Alias);

                if (powerBiFiltering != null)
                {
                    if (rql.Where == null)
                        rql.Where = powerBiFiltering;
                    else
                        rql.Where = new BinaryExpression(rql.Where, powerBiFiltering, OperatorType.And);

                    newRql = rql.ToString();
                }
                else
                {
                    newRql = rql.QueryText;
                }
            }
            else if (matches[0].Groups["table_name"].Success)
            {
                // SQL query coming from selecting an loading entire collection (table)

                if (matches.Count != 1)
                    throw new PgErrorException(PgErrorCodes.StatementTooComplex,
                        "Unexpected PowerBI nested SQL query. Query: " + queryText);

                var sqlQuery = matches[0];

                string tableName = sqlQuery.Groups["table_name"].Value;

                newRql = $"from '{tableName}'";
            }

            if (newRql == null)
            {
                pgQuery = null;
                return false;
            }

            var limit = matches[0].Groups["limit"];

            pgQuery = new PowerBIRqlQuery(newRql, parametersDataTypes, documentDatabase, powerBiReplaceValues, limit.Success ? int.Parse(limit.Value) : null);

            return true;
        }

        private static Dictionary<string, ReplaceColumnValue> GetReplaceValues(List<Match> matches)
        {
            Dictionary<string, ReplaceColumnValue> replaceValues = null;

            foreach (var matchToCheck in matches)
            {
                var replaceGroup = matchToCheck.Groups["replace"];

                if (replaceGroup.Success)
                {
                    if (string.IsNullOrEmpty(replaceGroup.Value) == false)
                    {
                        // Populate the replace columns starting from the inner-most SQL

                        replaceValues = new Dictionary<string, ReplaceColumnValue>();

                        for (var i = matches.Count - 1; i >= 0; i--)
                        {
                            replaceValues = GetReplaces(matches[i], ref replaceValues);
                        }

                        break;
                    }
                }
            }

            return replaceValues;
        }

        private static QueryExpression GetSqlWhereConditions(List<Match> matches, StringSegment? alias)
        {
            List<QueryExpression> whereExpressions = null;

            foreach (var matchToCheck in matches)
            {
                var whereGroup = matchToCheck.Groups["where"];

                if (whereGroup.Success)
                {
                    if (string.IsNullOrEmpty(whereGroup.Value) == false)
                    {
                        var whereFilteringCondition = whereGroup.Value;

                        var replaceValue = "${column}";

                        if (alias != null)
                            replaceValue = $"{alias}." + replaceValue;

                        whereFilteringCondition = WhereColumnRegex.Replace(whereFilteringCondition, replaceValue);

                        whereFilteringCondition = WhereOperatorRegex.Replace(whereFilteringCondition, (m) =>
                        {
                            if (OperatorMap.TryGetValue(m.Value, out var val))
                                return val;

                            return m.Value;
                        });

                        whereFilteringCondition = TimestampConditionRegex.Replace(whereFilteringCondition, timestampMatch =>
                        {
                            if (timestampMatch.Success)
                            {
                                var dateGroup = timestampMatch.Groups["date"];

                                if (dateGroup.Success && DateTime.TryParse(dateGroup.Value, out var date))
                                {
                                    return $"'{date.GetDefaultRavenFormat()}'";
                                }
                            }

                            return timestampMatch.Value;
                        });

                        var parser = new QueryParser();

                        parser.Init(whereFilteringCondition);

                        if (parser.Expression(out var parsedConditions) == false)
                        {
                            throw new NotSupportedException("Unable to parse WHERE clause: " + whereFilteringCondition);
                        }

                        whereExpressions ??= new List<QueryExpression>();

                        whereExpressions.Add(parsedConditions);
                    }
                }
            }

            if (whereExpressions == null)
                return null;

            if (whereExpressions.Count == 1)
                return whereExpressions[0];

            BinaryExpression result = null;

            for (int i = 1; i < whereExpressions.Count; i++)
            {
                if (result == null)
                    result = new BinaryExpression(whereExpressions[0], whereExpressions[1], OperatorType.And);
                else
                    result = new BinaryExpression(whereExpressions[i], result, OperatorType.And);
            }

            return result;
        }

        private static bool TryGetMatches(string queryText, out List<Match> outMatches, out Query rql)
        {
            var matches = new List<Match>();
            var queryToMatch = queryText;
            Group innerQuery;

            rql = null;

            // Queries can have inner queries that we need to parse, so here we collect those
            do
            {
                var match = FetchSqlRegex.Match(queryToMatch);

                if (!match.Success)
                {
                    outMatches = null;
                    return false;
                }

                matches.Add(match);

                innerQuery = match.Groups["inner_query"];
                queryToMatch = match.Groups["inner_query"].Value;
            } while (innerQuery.Success && !IsRql(queryToMatch, out rql));

            outMatches = matches;
            return true;
        }

        private static Dictionary<string, ReplaceColumnValue> GetReplaces(Match match, ref Dictionary<string, ReplaceColumnValue> replaces)
        {
            var destColumns = match.Groups["dest_columns"].Captures;
            var srcColumns = match.Groups["src_columns"].Captures;
            var replace = match.Groups["replace"].Captures;
            var replaceInputs = match.Groups["replace_inputs"].Captures;
            var replaceTexts = match.Groups["replace_texts"].Captures;

            var replaceIndex = 0;
            for (var i = 0; i < destColumns.Count; i++)
            {
                var destColumn = destColumns[i].Value;
                var srcColumn = srcColumns[i].Value;

                if (replace[i].Value.Length != 0)
                {
                    replaces[srcColumn] = new ReplaceColumnValue
                    {
                        DstColumnName = destColumn,
                        SrcColumnName = srcColumn,
                        OldValue = replaceInputs[replaceIndex].Value,
                        NewValue = replaceTexts[replaceIndex].Value,
                    };

                    replaceIndex++;
                }
            }

            return replaces;
        }

        private static bool IsRql(string queryText, out Query query)
        {
            try
            {
                query = QueryMetadata.ParseQuery(queryText, QueryType.Select);
            }
            catch
            {
                query = null;
                return false;
            }

            return true;
        }
    }
}

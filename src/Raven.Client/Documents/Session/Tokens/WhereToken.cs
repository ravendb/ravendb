//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Text;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    public class WhereToken : QueryToken
    {
        private WhereToken()
        {
        }

        public string FieldName { get; private set; }
        public WhereOperator WhereOperator { get; private set; }
        public SearchOperator? SearchOperator { get; set; }
        public string ParameterName { get; set; }
        public string FromParameterName { get; private set; }
        public string ToParameterName { get; private set; }
        public decimal? Boost { get; set; }
        public decimal? Fuzzy { get; set; }
        public int? Proximity { get; set; }
        public bool Exact { get; set; }

        public static WhereToken Equals(string fieldName, string parameterName, bool exact)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.Equals,
                Exact = exact
            };
        }

        public static WhereToken StartsWith(string fieldName, string parameterName)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.StartsWith
            };
        }

        public static WhereToken EndsWith(string fieldName, string parameterName)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.EndsWith
            };
        }

        public static WhereToken GreaterThan(string fieldName, string parameterName, bool exact)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.GreaterThan,
                Exact = exact
            };
        }

        public static WhereToken GreaterThanOrEqual(string fieldName, string parameterName, bool exact)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.GreaterThanOrEqual,
                Exact = exact
            };
        }

        public static WhereToken LessThan(string fieldName, string parameterName, bool exact)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.LessThan,
                Exact = exact
            };
        }

        public static WhereToken LessThanOrEqual(string fieldName, string parameterName, bool exact)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.LessThanOrEqual,
                Exact = exact
            };
        }

        public static WhereToken In(string fieldName, string parameterName, bool exact)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.In,
                Exact = exact
            };
        }

        public static WhereToken AllIn(string fieldName, string parameterName)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.AllIn
            };
        }

        public static WhereToken Between(string fieldName, string fromParameterName, string toParameterName, bool exact)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                FromParameterName = fromParameterName,
                ToParameterName = toParameterName,
                WhereOperator = WhereOperator.Between,
                Exact = exact
            };
        }

        public static WhereToken Search(string fieldName, string parameterName, SearchOperator op)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.Search,
                SearchOperator = op
            };
        }

        public static WhereToken Lucene(string fieldName, string parameterName)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = WhereOperator.Lucene
            };
        }

        public static QueryToken Exists(string fieldName)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = null,
                WhereOperator = WhereOperator.Exists
            };
        }
        
        public override void WriteTo(StringBuilder writer)
        {
            if (Boost.HasValue)
                writer.Append("boost(");

            if (Fuzzy.HasValue)
                writer.Append("fuzzy(");

            if (Proximity.HasValue)
                writer.Append("proximity(");

            if (Exact)
                writer.Append("exact(");

            switch (WhereOperator)
            {
                case WhereOperator.Search:
                    writer.Append("search(");
                    break;
                case WhereOperator.Lucene:
                    writer.Append("lucene(");
                    break;
                case WhereOperator.StartsWith:
                    writer.Append("startsWith(");
                    break;
                case WhereOperator.EndsWith:
                    writer.Append("endsWith(");
                    break;
                case WhereOperator.Exists:
                    writer.Append("exists(");
                    break;
            }

            writer.Append(FieldName);

            switch (WhereOperator)
            {
                case WhereOperator.In:
                    writer
                        .Append(" IN (:")
                        .Append(ParameterName)
                        .Append(")");
                    break;
                case WhereOperator.AllIn:
                    writer
                        .Append(" ALL IN (:")
                        .Append(ParameterName)
                        .Append(")");
                    break;
                case WhereOperator.Between:
                    writer
                        .Append(" BETWEEN :")
                        .Append(FromParameterName)
                        .Append(" AND :")
                        .Append(ToParameterName);
                    break;
                case WhereOperator.Equals:
                    writer
                        .Append(" = :")
                        .Append(ParameterName);
                    break;
                case WhereOperator.GreaterThan:
                    writer
                        .Append(" > :")
                        .Append(ParameterName);
                    break;
                case WhereOperator.GreaterThanOrEqual:
                    writer
                        .Append(" >= :")
                        .Append(ParameterName);
                    break;
                case WhereOperator.LessThan:
                    writer
                        .Append(" < :")
                        .Append(ParameterName);
                    break;
                case WhereOperator.LessThanOrEqual:
                    writer
                        .Append(" <= :")
                        .Append(ParameterName);
                    break;
                case WhereOperator.Search:
                    writer
                        .Append(", :")
                        .Append(ParameterName);

                    if (SearchOperator == Queries.SearchOperator.And)
                        writer.Append(", AND");

                    writer.Append(")");
                    break;
                case WhereOperator.Lucene:
                case WhereOperator.StartsWith:
                case WhereOperator.EndsWith:
                    writer
                        .Append(", :")
                        .Append(ParameterName)
                        .Append(")");
                    break;
                case WhereOperator.Exists:
                    writer
                        .Append(")");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            if (Exact)
                writer.Append(")");

            if (Proximity.HasValue)
            {
                writer
                    .Append(", ")
                    .Append(Proximity.Value.ToInvariantString())
                    .Append(")");
            }

            if (Fuzzy.HasValue)
            {
                writer
                    .Append(", ")
                    .Append(Fuzzy.Value.ToInvariantString())
                    .Append(")");
            }

            if (Boost.HasValue)
            {
                writer
                    .Append(", ")
                    .Append(Boost.Value.ToInvariantString())
                    .Append(")");
            }
        }
    }
}

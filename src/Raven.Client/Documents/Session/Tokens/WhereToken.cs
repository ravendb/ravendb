//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
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
        public string Value { get; private set; }
        public IEnumerable<object> Values { get; private set; }
        public string To { get; private set; }
        public string From { get; private set; }
        public decimal? Boost { get; set; }
        public decimal? Fuzzy { get; set; }
        public int? Proximity { get; set; }

        public static WhereToken Equals(string fieldName, string value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.Equals
            };
        }

        public static WhereToken StartsWith(string fieldName, string value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.StartsWith
            };
        }

        public static WhereToken EndsWith(string fieldName, string value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.EndsWith
            };
        }

        public static WhereToken GreaterThan(string fieldName, string value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.GreaterThan
            };
        }

        public static WhereToken GreaterThanOrEqual(string fieldName, string value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.GreaterThanOrEqual
            };
        }

        public static WhereToken LessThan(string fieldName, string value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.LessThan
            };
        }

        public static WhereToken LessThanOrEqual(string fieldName, string value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.LessThanOrEqual
            };
        }

        public static WhereToken In(string fieldName, IEnumerable<object> values)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Values = values,
                WhereOperator = WhereOperator.In
            };
        }

        public static WhereToken Between(string fieldName, string from, string to)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                From = from,
                To = to,
                WhereOperator = WhereOperator.Between
            };
        }

        public static WhereToken Search(string fieldName, string searchTerms)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = searchTerms,
                WhereOperator = WhereOperator.Search
            };
        }

        public static WhereToken ContainsAny(string fieldName, IEnumerable<object> values)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Values = values,
                WhereOperator = WhereOperator.ContainsAny
            };
        }

        public static WhereToken ContainsAll(string fieldName, IEnumerable<object> values)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Values = values,
                WhereOperator = WhereOperator.ContainsAll
            };
        }

        public static WhereToken Lucene(string fieldName, string whereClause)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = whereClause,
                WhereOperator = WhereOperator.Lucene
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

            if (WhereOperator == WhereOperator.Search)
                writer.Append("search(");

            if (WhereOperator == WhereOperator.Lucene)
                writer.Append("lucene(");

            if (WhereOperator == WhereOperator.StartsWith)
                writer.Append("startsWith(");

            if (WhereOperator == WhereOperator.EndsWith)
                writer.Append("endsWith(");

            writer.Append(RavenQuery.EscapeField(FieldName));

            switch (WhereOperator)
            {
                case WhereOperator.In:
                    writer
                        .Append(" IN (");

                    var first = true;
                    foreach (var value in Values)
                    {
                        if (first == false)
                            writer.Append(", ");

                        first = false;
                        writer.Append(value);
                    }

                    writer.Append(")");
                    break;
                case WhereOperator.Between:
                    writer
                        .Append(" BETWEEN ")
                        .Append(From)
                        .Append(" AND ")
                        .Append(To);
                    break;
                case WhereOperator.Equals:
                    writer
                        .Append(" = ")
                        .Append(Value);
                    break;
                case WhereOperator.GreaterThan:
                    writer
                        .Append(" > ")
                        .Append(Value);
                    break;
                case WhereOperator.GreaterThanOrEqual:
                    writer
                        .Append(" >= ")
                        .Append(Value);
                    break;
                case WhereOperator.LessThan:
                    writer
                        .Append(" < ")
                        .Append(Value);
                    break;
                case WhereOperator.LessThanOrEqual:
                    writer
                        .Append(" <= ")
                        .Append(Value);
                    break;
                case WhereOperator.Search:
                case WhereOperator.Lucene:
                case WhereOperator.StartsWith:
                case WhereOperator.EndsWith:
                    writer
                        .Append(", ")
                        .Append(Value)
                        .Append(")");
                    break;
                case WhereOperator.ContainsAny:
                    // TODO [ppekrol]
                    break;
                case WhereOperator.ContainsAll:
                    // TODO [ppekrol]
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

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

        public override QueryToken Clone()
        {
            return new WhereToken
            {
                Value = Value,
                FieldName = FieldName,
                Boost = Boost,
                Proximity = Proximity,
                WhereOperator = WhereOperator,
                Fuzzy = Fuzzy,
                From = From,
                To = To,
                Values = Values
            };
        }
    }
}

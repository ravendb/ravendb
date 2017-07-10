//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
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
        public object Value { get; private set; }
        public IEnumerable<object> Values { get; private set; }
        public object To { get; private set; }
        public object From { get; private set; }
        public decimal? Boost { get; set; }
        public decimal? Fuzzy { get; set; }
        public int? Proximity { get; set; }

        public static WhereToken Equals(string fieldName, object value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.Equals
            };
        }

        public static WhereToken StartsWith(string fieldName, object value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.StartsWith
            };
        }

        public static WhereToken EndsWith(string fieldName, object value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.EndsWith
            };
        }

        public static WhereToken GreaterThan(string fieldName, object value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.GreaterThan
            };
        }

        public static WhereToken GreaterThanOrEqual(string fieldName, object value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.GreaterThanOrEqual
            };
        }

        public static WhereToken LessThan(string fieldName, object value)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                Value = value,
                WhereOperator = WhereOperator.LessThan
            };
        }

        public static WhereToken LessThanOrEqual(string fieldName, object value)
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

        public static WhereToken Between(string fieldName, object from, object to)
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

                        WriteValue(writer, value);
                    }

                    writer.Append(")");
                    break;
                case WhereOperator.Between:
                    writer.Append(" BETWEEN ");
                    WriteValue(writer, From);
                    writer.Append(" AND ");
                    WriteValue(writer, To);
                    break;
                case WhereOperator.Equals:
                    writer.Append(" = ");
                    WriteValue(writer, Value);
                    break;
                case WhereOperator.GreaterThan:
                    writer.Append(" > ");
                    WriteValue(writer, Value);
                    break;
                case WhereOperator.GreaterThanOrEqual:
                    writer.Append(" >= ");
                    WriteValue(writer, Value);
                    break;
                case WhereOperator.LessThan:
                    writer.Append(" < ");
                    WriteValue(writer, Value);
                    break;
                case WhereOperator.LessThanOrEqual:
                    writer.Append(" <= ");
                    WriteValue(writer, Value);
                    break;
                case WhereOperator.Search:
                case WhereOperator.Lucene:
                case WhereOperator.StartsWith:
                case WhereOperator.EndsWith:
                    writer.Append(", ");
                    WriteValue(writer, Value);
                    writer.Append(")");
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

        private static void WriteValue(StringBuilder writer, object value)
        {
            if (value == null)
            {
                writer.Append("null");
                return;
            }

            if (value is bool)
            {
                writer.Append((bool)value ? "true" : "false");
                return;
            }

            if (value is int)
            {
                writer.Append(((int)value).ToInvariantString());
                return;
            }

            if (value is double)
            {
                writer.Append(((double)value).ToString("r", CultureInfo.InvariantCulture));
                return;
            }

            writer.Append("'");
            writer.Append(value);
            writer.Append("'");
        }
    }
}

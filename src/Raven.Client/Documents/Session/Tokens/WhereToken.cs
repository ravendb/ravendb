//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Text;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    public class WhereToken : QueryToken
    {
        protected WhereToken()
        {
        }

        public enum MethodsType
        {
            CmpXchg
        }

        public class WhereMethodCall
        {
            public MethodsType MethodType;
            public string[] Parameters;
            public string Property;
        }
        
        public class WhereOptions
        {
            public SearchOperator? SearchOperator{ get; private set; }
            public string FromParameterName{ get; private set; }
            public string ToParameterName{ get; private set; }
            public decimal? Boost { get; set; }
            public decimal? Fuzzy{ get; set; }
            public int? Proximity{ get; set; }
            public bool Exact{ get; private set; }
            public WhereMethodCall Method { get; private set; }
            public ShapeToken WhereShape{ get; private set; }
            public double DistanceErrorPct{ get; private set; }

            private WhereOptions(){}

            public static WhereOptions Default() => new WhereOptions();

            public WhereOptions(bool exact)
            {
                Exact = exact;
            }

            public WhereOptions(bool exact, string from, string to)
            {
                Exact = exact;
                FromParameterName = from;
                ToParameterName = to;
            }

            public WhereOptions(SearchOperator search)
            {
                SearchOperator = search;
            }

            public WhereOptions(ShapeToken shape, double distance)
            {
                WhereShape = shape;
                DistanceErrorPct = distance;
            }

            public WhereOptions(MethodsType methodType, string[] parameters, string property, bool exact = false)
            {
                Method = new WhereMethodCall
                {
                    MethodType = methodType,
                    Parameters = parameters,
                    Property = property
                };

                Exact = exact;
            }
        }

        public string FieldName { get; private set; }
        public WhereOperator WhereOperator { get; private set; }
        public string ParameterName { get; private set; }
        public WhereOptions Options;
        
        
        public static WhereToken Create(WhereOperator op, string fieldName, string parameterName, WhereOptions options = null)
        {
            return new WhereToken
            {
                FieldName = fieldName,
                ParameterName = parameterName,
                WhereOperator = op,
                Options = options ?? WhereOptions.Default()
            };
        }

        public WhereToken AddAlias(string alias)
        {
            if (FieldName == "id()")
                return this;

            return new WhereToken
            {
                FieldName = alias + "." + FieldName,
                ParameterName = ParameterName,
                WhereOperator = WhereOperator,
                Options = Options
            };
        }

        private bool WriteMethod(StringBuilder writer)
        {
            if (Options.Method != null)
            {
                switch (Options.Method.MethodType)
                {
                    case MethodsType.CmpXchg:
                        writer.Append("cmpxchg(");
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                var first = true;
                foreach (var parameter in Options.Method.Parameters)
                {
                    if (first == false)
                        writer.Append(",");
                    first = false;
                    writer.Append("$");
                    writer.Append(parameter);
                }
                writer.Append(")");
                if (Options.Method.Property != null)
                {
                    writer.Append(".").Append(Options.Method.Property);
                }
                return true;
            }
            return false;
        }
        
        
        public override void WriteTo(StringBuilder writer)
        {
            if (Options.Boost.HasValue)
                writer.Append("boost(");

            if (Options.Fuzzy.HasValue)
                writer.Append("fuzzy(");

            if (Options.Proximity.HasValue)
                writer.Append("proximity(");

            if (Options.Exact)
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
                case WhereOperator.Spatial_Within:
                    writer.Append("spatial.within(");
                    break;
                case WhereOperator.Spatial_Contains:
                    writer.Append("spatial.contains(");
                    break;
                case WhereOperator.Spatial_Disjoint:
                    writer.Append("spatial.disjoint(");
                    break;
                case WhereOperator.Spatial_Intersects:
                    writer.Append("spatial.intersects(");
                    break;
                case WhereOperator.Regex:
                    writer.Append("regex(");
                    break;
            }

            WriteInnerWhere(writer);

            if (Options.Exact)
                writer.Append(")");

            if (Options.Proximity.HasValue)
            {
                writer
                    .Append(", ")
                    .Append(Options.Proximity.Value.ToInvariantString())
                    .Append(")");
            }

            if (Options.Fuzzy.HasValue)
            {
                writer
                    .Append(", ")
                    .Append(Options.Fuzzy.Value.ToInvariantString())
                    .Append(")");
            }

            if (Options.Boost.HasValue)
            {
                writer
                    .Append(", ")
                    .Append(Options.Boost.Value.ToInvariantString())
                    .Append(")");
            }
        }

        private void WriteInnerWhere(StringBuilder writer)
        {
            WriteField(writer, FieldName);

            switch (WhereOperator)
            {
                case WhereOperator.Equals:
                    writer
                        .Append(" = ");
                    break;
                case WhereOperator.NotEquals:
                    writer
                        .Append(" != ");
                    break;
                case WhereOperator.GreaterThan:
                    writer
                        .Append(" > ");
                    break;
                case WhereOperator.GreaterThanOrEqual:
                    writer
                        .Append(" >= ");
                    break;
                case WhereOperator.LessThan:
                    writer
                        .Append(" < ");
                    break;
                case WhereOperator.LessThanOrEqual:
                    writer
                        .Append(" <= ");
                    break;
                default:
                    SpecialOperator(writer);
                    return;
            }

            if (WriteMethod(writer) == false)
            {
                writer.Append("$").Append(ParameterName);
            }
        }

        private void SpecialOperator(StringBuilder writer)
        {
            switch (WhereOperator)
            {
                case WhereOperator.In:
                    writer
                        .Append(" in ($")
                        .Append(ParameterName)
                        .Append(")");
                    break;
                case WhereOperator.AllIn:
                    writer
                        .Append(" all in ($")
                        .Append(ParameterName)
                        .Append(")");
                    break;
                case WhereOperator.Between:
                    writer
                        .Append(" between $")
                        .Append(Options.FromParameterName)
                        .Append(" and $")
                        .Append(Options.ToParameterName);
                    break;
                case WhereOperator.Search:
                    writer
                        .Append(", $")
                        .Append(ParameterName);

                    if (Options.SearchOperator == SearchOperator.And)
                        writer.Append(", and");

                    writer.Append(")");
                    break;
                case WhereOperator.Lucene:
                case WhereOperator.StartsWith:
                case WhereOperator.EndsWith:
                case WhereOperator.Regex:
                    writer
                        .Append(", $")
                        .Append(ParameterName)
                        .Append(")");
                    break;
                case WhereOperator.Exists:
                    writer
                        .Append(")");
                    break;
                case WhereOperator.Spatial_Within:
                case WhereOperator.Spatial_Contains:
                case WhereOperator.Spatial_Disjoint:
                case WhereOperator.Spatial_Intersects:
                    writer
                        .Append(", ");

                    Options.WhereShape.WriteTo(writer);

                    if (Options.DistanceErrorPct.AlmostEquals(Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct) == false)
                    {
                        writer.Append(", ");
                        writer.Append(Options.DistanceErrorPct);
                    }

                    writer
                        .Append(")");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(WhereOperator), WhereOperator, "Unexpected operator");
            }
        }
    }
}

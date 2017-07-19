using System;
using System.Collections.Generic;
using System.Globalization;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class RangeLuceneASTNode : LuceneASTNodeBase
    {
        private bool _maxIsNull;
        private bool _minIsNull;
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield break; }
        }
        public override global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            switch (configuration.FieldName.Type)
            {
                case FieldName.FieldType.String:
                    var minTermIsNullOrStar = RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term.Equals(Asterisk);
                    var maxTermIsNullOrStar = RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term.Equals(Asterisk);
                    if (minTermIsNullOrStar && maxTermIsNullOrStar)
                    {
                        return new WildcardQuery(new Term(configuration.FieldName.Field, Asterisk));
                    }
                    return new TermRangeQuery(configuration.FieldName.Field,
                        minTermIsNullOrStar ? null : RangeMin.Term,
                        maxTermIsNullOrStar ? null : RangeMax.Term,
                        InclusiveMin, InclusiveMax);
                case FieldName.FieldType.Long:
                    OverrideInclusiveForKnownNumericRange();
                    var longMin = _minIsNull ? long.MinValue : ParseTermToLong(RangeMin);
                    var longMax = _maxIsNull ? long.MaxValue : ParseTermToLong(RangeMax);
                    return NumericRangeQuery.NewLongRange(configuration.FieldName.Field, 4, longMin, longMax, InclusiveMin, InclusiveMax);
                case FieldName.FieldType.Double:
                    OverrideInclusiveForKnownNumericRange();
                    var doubleMin = _minIsNull ? double.MinValue : double.Parse(RangeMin.Term);
                    var doubleMax = _maxIsNull ? double.MaxValue : double.Parse(RangeMax.Term);
                    return NumericRangeQuery.NewDoubleRange(configuration.FieldName.Field, 4, doubleMin, doubleMax, InclusiveMin, InclusiveMax);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private long? ParseTermToLong(TermLuceneASTNode rangeMin)
        {
            return rangeMin.Type == TermLuceneASTNode.TermType.Hex
                ? long.Parse(rangeMin.Term.Substring(2), NumberStyles.HexNumber)
                : long.Parse(rangeMin.Term);
        }


        /// <summary>
        /// For numeric values { NUll TO <number/> } should be [ <min-value/> TO <number/>} but not for string values.
        /// </summary>
        private void OverrideInclusiveForKnownNumericRange()
        {
            if (RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term == Asterisk)
            {
                _maxIsNull = true;
                InclusiveMax = true;
            }
            if (RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term == Asterisk)
            {
                _minIsNull = true;
                InclusiveMin = true;
            }
        }


        public TermLuceneASTNode RangeMin { get; set; }
        public TermLuceneASTNode RangeMax { get; set; }
        public bool InclusiveMin { get; set; }
        public bool InclusiveMax { get; set; }
        public override string ToString()
        {
            return $"{(InclusiveMin ? '[' : '{')}{RangeMin} TO {RangeMax}{(InclusiveMax ? ']' : '}')}";
        }
    }
}
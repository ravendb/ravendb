using System;
using System.Text.RegularExpressions;

using Lucene.Net.Analysis;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{
	[CLSCompliant(false)]
	public class RangeQueryParser : QueryParser
	{
		static readonly Regex rangeValue = new Regex(@"^[\w\d]x[\w\d.]+$", RegexOptions.Compiled);

		public RangeQueryParser(Version matchVersion, string f, Analyzer a)
			: base(matchVersion, f, a)
		{
		}

		/// <summary>
		/// Detects numeric range terms and expands range expressions accordingly
		/// </summary>
		/// <param name="field"></param>
		/// <param name="lower"></param>
		/// <param name="upper"></param>
		/// <param name="inclusive"></param>
		/// <returns></returns>
		protected override Query GetRangeQuery(string field, string lower, string upper, bool inclusive)
		{
			if (!rangeValue.IsMatch(lower) && !rangeValue.IsMatch(upper))
			{
				return base.GetRangeQuery(field, lower, upper, inclusive);
			}

			var from = NumberUtil.StringToNumber(lower);
			var to = NumberUtil.StringToNumber(upper);

			TypeCode numericType;

			if (from != null)
				numericType = Type.GetTypeCode(from.GetType());
			else if (to != null)
				numericType = Type.GetTypeCode(to.GetType());
			else
				numericType = TypeCode.Empty;

			switch (numericType)
			{
				case TypeCode.Int32:
				{
					return NumericRangeQuery.NewIntRange(field, (int)(from ?? Int32.MinValue), (int)(to ?? Int32.MaxValue), inclusive, inclusive);
				}
				case TypeCode.Int64:
				{
					return NumericRangeQuery.NewLongRange(field, (long)(from ?? Int64.MinValue), (long)(to ?? Int64.MaxValue), inclusive, inclusive);
				}
				case TypeCode.Double:
				{
					return NumericRangeQuery.NewDoubleRange(field, (double)(from ?? Double.MinValue), (double)(to ?? Double.MaxValue), inclusive, inclusive);
				}
				case TypeCode.Single:
				{
					return NumericRangeQuery.NewFloatRange(field, (float)(from ?? Single.MinValue), (float)(to ?? Single.MaxValue), inclusive, inclusive);
				}
				default:
				{
					return base.GetRangeQuery(field, lower, upper, inclusive);
				}
			}
		}
	}
}

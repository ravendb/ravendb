using System;
using System.Text;
using Raven.Client.Documents.Operations.TimeSeries;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class TimeSeriesIncludesToken : QueryToken
    {
        private string _sourcePath;
        private readonly AbstractTimeSeriesRange _range;

        private TimeSeriesIncludesToken(string sourcePath, AbstractTimeSeriesRange range)
        {
            _range = range;
            _sourcePath = sourcePath;
        }

        public static TimeSeriesIncludesToken Create(string sourcePath, AbstractTimeSeriesRange range)
        {
            return new TimeSeriesIncludesToken(sourcePath, range);
        }

        public void AddAliasToPath(string alias)
        {
            _sourcePath = _sourcePath == string.Empty
                ? alias
                : $"{alias}.{_sourcePath}";
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("timeseries(");

            if (_sourcePath != string.Empty)
            {
                writer
                    .Append(_sourcePath)
                    .Append(", ");
            }

            if (_range.Name != string.Empty)
            {
                writer.Append("'")
                    .Append(_range.Name)
                    .Append("'")
                    .Append(", ");
            }

            switch (_range)
            {
                case TimeSeriesRange r:
                    WriteTo(writer, r);
                    break;
                case TimeSeriesTimeRange tr:
                    WriteTo(writer, tr);
                    break;
                case TimeSeriesCountRange cr:
                    WriteTo(writer, cr);
                    break;
                default:
                    throw new NotSupportedException($"Not supported time range type '{_range?.GetType().Name}'.");
            };

            writer.Append(")");
        }

        private static void WriteTo(StringBuilder writer, TimeSeriesTimeRange range)
        {
            switch (range.Type)
            {
                case TimeSeriesRangeType.Last:
                    writer
                        .Append("last(");
                    break;
                default:
                    throw new NotSupportedException($"Not supported time range type '{range.Type}'.");
            }

            writer
                .Append(range.Time.Value)
                .Append(", '")
                .Append(range.Time.Unit)
                .Append("')");
        }

        private static void WriteTo(StringBuilder writer, TimeSeriesCountRange range)
        {
            switch (range.Type)
            {
                case TimeSeriesRangeType.Last:
                    writer
                        .Append("last(");
                    break;
                default:
                    throw new NotSupportedException($"Not supported time range type '{range.Type}'.");
            }

            writer
                .Append(range.Count)
                .Append(")");
        }

        private static void WriteTo(StringBuilder writer, TimeSeriesRange range)
        {
            if (range.From.HasValue)
            {
                writer.Append("'")
                    .Append(range.From.Value.GetDefaultRavenFormat())
                    .Append("'")
                    .Append(", ");
            }
            else
            {
                writer.Append("null,");
            }

            if (range.To.HasValue)
            {
                writer.Append("'")
                    .Append(range.To.Value.GetDefaultRavenFormat())
                    .Append("'");
            }
            else
            {
                writer.Append("null");
            }
        }
    }
}

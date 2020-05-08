using System;
using System.Text;
using Raven.Client.Documents.Operations.TimeSeries;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    internal class TimeSeriesIncludesToken : QueryToken
    {
        private string _sourcePath;
        private readonly TimeSeriesRange _range;

        private TimeSeriesIncludesToken(string sourcePath, TimeSeriesRange range)
        {
            _range = range;
            _sourcePath = sourcePath;
        }

        public static TimeSeriesIncludesToken Create(string sourcePath, TimeSeriesRange range)
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
                writer.Append(_sourcePath).Append(", ");
            }

            writer.Append("'")
                .Append(_range.Name)
                .Append("'")
                .Append(", ");

            if (_range.From.HasValue)
            {
                writer.Append("'")
                    .Append(_range.From.Value.GetDefaultRavenFormat())
                    .Append("'")
                    .Append(", ");
            }
            else
            {
                writer.Append("null,");
            }

            if (_range.To.HasValue)
            {
                
                writer.Append("'")
                    .Append(_range.To.Value.GetDefaultRavenFormat())
                    .Append("'");
            }
            else
            {
                writer.Append("null");
            }

            writer.Append(")");
        }
    }
}

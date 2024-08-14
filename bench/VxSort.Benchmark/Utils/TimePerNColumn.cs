using System.Globalization;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Horology;

namespace Bench.Utils
{
    public static class CommonExtensions
    {
        public static string ToTimeStr(
            this double value,
            TimeUnit unit = null,
            int unitNameWidth = 1,
            bool showUnit = true,
            string format = "N4",
            Encoding encoding = null
        )
        {
            unit = unit ?? TimeUnit.GetBestTimeUnit(value);
            double unitValue = TimeUnit.Convert(value, TimeUnit.Nanosecond, unit);
            if (showUnit)
            {
                string unitName = unit.FullName.PadLeft(unitNameWidth);
                return $"{unitValue.ToStr(format)} {unitName}";
            }

            return $"{unitValue.ToStr(format)}";
        }

        public static string ToStr(this double value, string format = "0.##")
        {
            // Here we should manually create an object[] for string.Format
            // If we write something like
            //     string.Format(HostEnvironmentInfo.MainCultureInfo, $"{{0:{format}}}", value)
            // it will be resolved to:
            //     string.Format(System.IFormatProvider, string, params object[]) // .NET 4.5
            //     string.Format(System.IFormatProvider, string, object)          // .NET 4.6
            // Unfortunately, Mono doesn't have the second overload (with object instead of params object[]).
            var args = new object[] { value };
            return string.Format(CultureInfo.InvariantCulture, $"{{0:{format}}}", args);
        }

        public static string ToTimeStr(
            this double value,
            TimeUnit unit,
            Encoding encoding,
            string format = "N4",
            int unitNameWidth = 1,
            bool showUnit = true
        ) => value.ToTimeStr(unit, unitNameWidth, showUnit, format, encoding);

        public static string ToTimeStr(
            this double value,
            Encoding encoding,
            TimeUnit unit = null,
            string format = "N4",
            int unitNameWidth = 1,
            bool showUnit = true
        ) => value.ToTimeStr(unit, unitNameWidth, showUnit, format, encoding);
    }

    public class TimePerNColumn : IColumn
    {
        public string Id => nameof(TimePerNColumn);
        public string ColumnName => "Time / N";

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            return "";
        }

        public bool IsAvailable(Summary summary) => true;

        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Statistics;
        public int PriorityInCategory => 0;
        public bool IsNumeric => true;
        public UnitType UnitType => UnitType.Time;
        public string Legend => $"Time taken to process a single element";

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            var valueOfN = (int)benchmarkCase.Parameters.Items.Single(p => p.Name == "N").Value;
            var timePerN = summary[benchmarkCase].ResultStatistics.Mean / valueOfN;
            return timePerN.ToTimeStr(TimeUnit.GetBestTimeUnit(timePerN));
        }

        public override string ToString() => ColumnName;
    }
}

using System;
using System.Globalization;
using System.Text;
using metrics.Core;
using metrics.Util;

namespace metrics.Reporting
{
    public class HumanReadableReportFormatter : IReportFormatter
    {
        private readonly Metrics _metrics;

        public HumanReadableReportFormatter(Metrics metrics)
        {
            _metrics = metrics;
        }

        public string GetSample()
        {
            var sb = new StringBuilder();
            var now = DateTime.Now;
            var dateTime = string.Format("{0} {1}", now.ToString("d"), now.ToString("d"));
            sb.Append(dateTime);
            sb.Append(' ');
            for (var i = 0; i < (80 - dateTime.Length - 1); i++)
            {
                sb.Append('=');
            }
            sb.AppendLine();

            foreach (var entry in Utils.SortMetrics(_metrics.All))
            {
                sb.Append(entry.Key);
                sb.AppendLine(":");

                foreach (var subEntry in entry.Value)
                {
                    sb.Append("  ");
                    sb.Append(subEntry.Key);
                    sb.AppendLine(":");

                    var metric = subEntry.Value;
                    if (metric is GaugeMetric)
                    {
                        WriteGauge(sb, (GaugeMetric)metric);
                    }
                    else if (metric is CounterMetric)
                    {
                        WriteCounter(sb, (CounterMetric)metric);
                    }
                    else if (metric is HistogramMetric)
                    {
                        WriteHistogram(sb, (HistogramMetric)metric);
                    }
                    else if (metric is MeterMetric)
                    {
                        WriteMetered(sb, (MeterMetric)metric);
                    }
                    else if (metric is TimerMetricBase)
                    {
                        WriteTimer(sb, (TimerMetricBase)metric);
                    }
                    sb.AppendLine();
                }
                sb.AppendLine();
            }

            return sb.ToString();
        }

        protected void WriteGauge(StringBuilder sb, GaugeMetric gauge)
        {
            sb.Append("    value = ");
            sb.AppendLine(gauge.ValueAsString);
        }

        protected void WriteCounter(StringBuilder sb, CounterMetric counter)
        {
            sb.Append("    count = ");
            sb.AppendLine(counter.Count.ToString());
        }

        protected void WriteMetered(StringBuilder sb, IMetered meter)
        {
            var unit = Abbreviate(meter.RateUnit);
            sb.AppendFormat("             count = {0}\n", meter.Count);
            sb.AppendFormat("         mean rate = {0} {1}/{2}\n", meter.MeanRate, meter.EventType, unit);
            sb.AppendFormat("     1-minute rate = {0} {1}/{2}\n", meter.OneMinuteRate, meter.EventType, unit);
            sb.AppendFormat("     5-minute rate = {0} {1}/{2}\n", meter.FiveMinuteRate, meter.EventType, unit);
            sb.AppendFormat("    15-minute rate = {0} {1}/{2}\n", meter.FifteenMinuteRate, meter.EventType, unit);
        }

        protected void WriteHistogram(StringBuilder sb, HistogramMetric histogram)
        {
            var percentiles = histogram.Percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999);

            sb.AppendFormat("               min = %{0:F2}\n", histogram.Min);
            sb.AppendFormat("               max = %{0:F2}\n", histogram.Max);
            sb.AppendFormat("              mean = %{0:F2}\n", histogram.Mean);
            sb.AppendFormat("            stddev = %{0:F2}\n", histogram.StdDev);
            sb.AppendFormat("            median = %{0:F2}\n", percentiles[0]);
            sb.AppendFormat("              75%% <= %{0:F2}\n", percentiles[1]);
            sb.AppendFormat("              95%% <= %{0:F2}\n", percentiles[2]);
            sb.AppendFormat("              98%% <= %{0:F2}\n", percentiles[3]);
            sb.AppendFormat("              99%% <= %{0:F2}\n", percentiles[4]);
            sb.AppendFormat("            99.9%% <= %{0:F2}\n", percentiles[5]);
        }

        protected void WriteTimer(StringBuilder sb, TimerMetricBase timer)
        {
            WriteMetered(sb, timer);

            var durationUnit = Abbreviate(timer.DurationUnit);

            var percentiles = timer.Percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999);

            sb.AppendFormat("               min = %{0:F2}{1}\n", timer.Min, durationUnit);
            sb.AppendFormat("               max = %{0:F2}{1}\n", timer.Max, durationUnit);
            sb.AppendFormat("              mean = %{0:F2}{1}\n", timer.Mean, durationUnit);
            sb.AppendFormat("            stddev = %{0:F2}{1}\n", timer.StdDev, durationUnit);
            sb.AppendFormat("            median = %{0:F2}{1}\n", percentiles[0], durationUnit);
            sb.AppendFormat("              75%% <= %{0:F2}{1}\n", percentiles[1], durationUnit);
            sb.AppendFormat("              95%% <= %{0:F2}{1}\n", percentiles[2], durationUnit);
            sb.AppendFormat("              98%% <= %{0:F2}{1}\n", percentiles[3], durationUnit);
            sb.AppendFormat("              99%% <= %{0:F2}{1}\n", percentiles[4], durationUnit);
            sb.AppendFormat("            99.9%% <= %{0:F2}{1}\n", percentiles[5], durationUnit);
        }

        protected static string Abbreviate(TimeUnit unit)
        {
            switch (unit)
            {
                case TimeUnit.Nanoseconds:
                    return "ns";
                case TimeUnit.Microseconds:
                    return "us";
                case TimeUnit.Milliseconds:
                    return "ms";
                case TimeUnit.Seconds:
                    return "s";
                case TimeUnit.Minutes:
                    return "m";
                case TimeUnit.Hours:
                    return "h";
                case TimeUnit.Days:
                    return "d";
                default:
                    throw new ArgumentOutOfRangeException("unit");
            }
        }
    }
}

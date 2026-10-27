using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Zstd.Benchmark.EndToEnd;

namespace Zstd.Benchmark.Reporting
{
    /// <summary>
    /// Markdown report of an end-to-end run. Every configuration is compared with 0 workers at the same compression level,
    /// separately in each round; an effect is confirmed only when all rounds agree.
    /// </summary>
    internal static class EndToEndReport
    {
        private const double Threshold = 0.05;

        private static readonly string[] Operations = { EndToEndRunner.Export, EndToEndRunner.Backup, EndToEndRunner.Snapshot, EndToEndRunner.Restore, EndToEndRunner.Import };

        public static string Create(EndToEndRun run)
        {
            List<EndToEndMeasurement> all = run.Measurements;
            int[] rounds = all.Select(x => x.Round).Distinct().OrderBy(x => x).ToArray();
            StringBuilder sb = new();
            sb.AppendLine("# zstd workers end to end: export, backup, snapshot, restore, import");
            sb.AppendLine();
            sb.AppendLine($"- Started: {run.Started}");
            sb.AppendLine($"- Machine: {run.Machine}");
            sb.AppendLine($"- Server: separate process, affinity: {run.ServerAffinity}; license: {run.License}");
            if (run.Dataset != null)
                sb.AppendLine($"- Database: {run.Dataset.Documents:N0} documents (mutated Northwind), {run.Dataset.JsonBytes / (double)(1L << 30):F2} GiB of JSON");
            sb.AppendLine($"- {run.Iterations} measured iterations per configuration and round (median shown, ± is half the min-max range), " +
                          $"{rounds.Length} round(s) in alternating order, a fresh server per configuration, one unmeasured export first");
            sb.AppendLine("- Restore / import: once per round, Fastest only (restores the logical backup, imports the export of that configuration)");
            sb.AppendLine($"- Verdict vs 0 workers at the same level: faster / slower when every round agrees by more than {Threshold:P0} and more than " +
                          $"the spread within the round; no difference when every round is within {Threshold:P0}; otherwise unclear");
            sb.AppendLine();

            sb.AppendLine("## Duration");
            sb.AppendLine();
            sb.Append("| Operation | Level | Workers |");
            foreach (int round in rounds)
                sb.Append($" Round {round} |");
            sb.AppendLine(" vs 0 workers | Verdict |");
            sb.Append("|---|---|---:|");
            foreach (int _ in rounds)
                sb.Append("---:|");
            sb.AppendLine("---:|---|");

            foreach ((string operation, string level, int workers, List<EndToEndMeasurement> rows) in Groups(all))
            {
                sb.Append($"| {operation} | {level} | {workers} |");
                foreach (int round in rounds)
                {
                    List<double> values = rows.Where(x => x.Round == round).Select(x => x.Seconds).ToList();
                    sb.Append(values.Count == 0 ? " |" : $" {Median(values):F2} s{Spread(values)} |");
                }

                if (workers == 0)
                {
                    sb.AppendLine(" | |");
                    continue;
                }

                List<EndToEndMeasurement> baseline = all.Where(x => x.Operation == operation && x.Level == level && x.Workers == 0).ToList();
                (string changes, string verdict) = Compare(rounds, baseline, rows, x => x.Seconds);
                sb.AppendLine($" {changes} | {verdict} |");
            }

            sb.AppendLine();
            sb.AppendLine("## Server CPU time, memory and output size");
            sb.AppendLine();
            sb.AppendLine("Peak memory: highest private bytes of the server process during the operation, minus private bytes when it started.");
            sb.AppendLine();
            sb.AppendLine("| Operation | Level | Workers | CPU time (per round) | CPU vs 0 workers | Peak memory (per round) | Output size |");
            sb.AppendLine("|---|---|---:|---:|---:|---:|---:|");
            foreach ((string operation, string level, int workers, List<EndToEndMeasurement> rows) in Groups(all))
            {
                string cpu = string.Join(" / ", rounds.Select(r => rows.Where(x => x.Round == r).Select(x => x.ServerCpuSeconds).ToList())
                    .Where(v => v.Count > 0).Select(v => $"{Median(v):F1} s"));
                string memory = string.Join(" / ", rounds.Select(r => rows.Where(x => x.Round == r).Select(x => (double)(x.ServerPeakPrivateBytes - x.ServerPrivateBytesBefore)).ToList())
                    .Where(v => v.Count > 0).Select(v => $"{Median(v) / 1048576:F0} MB"));
                List<double> sizes = rows.Where(x => x.OutputBytes > 0).Select(x => (double)x.OutputBytes).ToList();
                string size = sizes.Count == 0 ? "" : $"{Median(sizes) / 1048576:F1} MB";

                string cpuChange = "";
                if (workers != 0)
                {
                    List<EndToEndMeasurement> baseline = all.Where(x => x.Operation == operation && x.Level == level && x.Workers == 0).ToList();
                    cpuChange = Compare(rounds, baseline, rows, x => x.ServerCpuSeconds).Changes;
                }

                sb.AppendLine($"| {operation} | {level} | {workers} | {cpu} | {cpuChange} | {memory} | {size} |");
            }

            return sb.ToString();
        }

        private static IEnumerable<(string Operation, string Level, int Workers, List<EndToEndMeasurement> Rows)> Groups(List<EndToEndMeasurement> all) =>
            all.GroupBy(x => (x.Operation, x.Level, x.Workers))
                .OrderBy(g => Array.IndexOf(Operations, g.Key.Operation))
                .ThenBy(g => g.Key.Level, StringComparer.Ordinal)
                .ThenBy(g => g.Key.Workers)
                .Select(g => (g.Key.Operation, g.Key.Level, g.Key.Workers, g.ToList()));

        private static (string Changes, string Verdict) Compare(int[] rounds, List<EndToEndMeasurement> baseline, List<EndToEndMeasurement> candidate,
            Func<EndToEndMeasurement, double> metric)
        {
            List<string> changes = new();
            List<double> values = new();
            List<double> noise = new();
            foreach (int round in rounds)
            {
                List<double> b = baseline.Where(x => x.Round == round).Select(metric).ToList();
                List<double> c = candidate.Where(x => x.Round == round).Select(metric).ToList();
                if (b.Count == 0 || c.Count == 0)
                    continue;

                double change = Median(c) / Median(b) - 1;
                values.Add(change);
                noise.Add(Math.Max(Range(b), Range(c)));
                changes.Add(change.ToString("+0%;-0%;0%", CultureInfo.InvariantCulture));
            }

            if (values.Count == 0)
                return ("", "");

            string verdict;
            if (values.Select((v, i) => Math.Abs(v) > Math.Max(Threshold, noise[i])).All(x => x) && (values.All(v => v < 0) || values.All(v => v > 0)))
                verdict = values[0] < 0 ? "**faster**" : "**slower**";
            else if (values.All(v => Math.Abs(v) < Threshold))
                verdict = "no difference";
            else
                verdict = "unclear";

            if (values.Count == 1)
                verdict += " (1 round)";

            return (string.Join(" / ", changes), verdict);
        }

        private static double Median(List<double> values)
        {
            List<double> sorted = values.OrderBy(x => x).ToList();
            int middle = sorted.Count / 2;
            return sorted.Count % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
        }

        private static double Range(List<double> values) => values.Count < 2 ? 0 : (values.Max() - values.Min()) / Median(values);

        private static string Spread(List<double> values) => values.Count < 2 ? "" : $" ±{Range(values) / 2:P0}";
    }
}

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Zstd.Benchmark.Reporting
{
    internal sealed class ComparisonOptions
    {
        public double Threshold = 0.03;

        /// <summary>Pair every <c>X_Baseline</c> method (frozen pre-change code) with <c>X</c> (production code).</summary>
        public bool InRun;

        public string BaselineJob;
        public string CandidateJob;

        /// <summary>The same comparison on a second, independent run; only effects both runs agree on are confirmed.</summary>
        public string ConfirmWith;
    }

    /// <summary>
    /// Compares BenchmarkDotNet full JSON reports (*-report-full.json): two runs, two jobs of one run (libzstd A vs B),
    /// frozen pre-change code vs production code in the same run, or old code on job A vs new code on job B.
    /// A difference is called out only when the 99.9% confidence intervals do not overlap AND the means differ by more
    /// than the threshold. With a second run, an effect is confirmed only when both runs report it.
    /// </summary>
    internal static class ResultsComparer
    {
        public const string BaselineMethodSuffix = "_Baseline";

        private sealed class Result
        {
            public string Type;
            public string Method;
            public string Parameters;
            public string Job;
            public double Mean;
            public double Lower;
            public double Upper;
            public double? Allocated;
        }

        private sealed class Row
        {
            public string Key;
            public string Benchmark;
            public string Parameters;
            public string Job;
            public Result Baseline;
            public Result Candidate;
            public double Change;
            public string Verdict;
        }

        public static string Compare(string baselinePath, string candidatePath, ComparisonOptions options)
        {
            List<Row> rows = CreateRows(baselinePath, candidatePath, options);
            List<Row> confirmation = null;
            if (options.ConfirmWith != null)
            {
                // comparing within one run: the repeat is another run; comparing two runs: the repeat is "<baseline>;<candidate>"
                string[] repeat = options.ConfirmWith.Split(';');
                confirmation = repeat.Length == 2
                    ? CreateRows(repeat[0], repeat[1], options)
                    : CreateRows(options.ConfirmWith, options.ConfirmWith, options);
            }

            StringBuilder sb = new();
            sb.AppendLine("# Benchmark comparison").AppendLine();
            sb.AppendLine($"- baseline: {Describe(baselinePath, options, baseline: true)}");
            sb.AppendLine($"- candidate: {Describe(candidatePath, options, baseline: false)}");
            if (confirmation != null)
                sb.AppendLine($"- repeated on an independent run: `{options.ConfirmWith}`");
            sb.AppendLine($"- verdict per run: faster/slower only when 99.9% confidence intervals do not overlap and |change| > {options.Threshold:P0}");
            if (confirmation != null)
                sb.AppendLine("- confirmed: both runs give the same verdict; anything else is reported as inconsistent (noise)");
            sb.AppendLine();

            if (confirmation == null)
            {
                sb.AppendLine("| Benchmark | Parameters | Job | Baseline | Candidate | Change | Verdict | Alloc base | Alloc cand |");
                sb.AppendLine("|---|---|---|---:|---:|---:|---|---:|---:|");
                foreach (Row row in rows)
                {
                    sb.AppendLine(string.Create(CultureInfo.InvariantCulture,
                        $"| {row.Benchmark} | {row.Parameters} | {row.Job} | {FormatTime(row.Baseline.Mean)} | {FormatTime(row.Candidate.Mean)} | {row.Change:+0.0%;-0.0%;0.0%} | {Bold(row.Verdict)} | {FormatBytes(row.Baseline.Allocated)} | {FormatBytes(row.Candidate.Allocated)} |"));
                }

                sb.AppendLine();
                sb.AppendLine(Summary(rows.Select(x => x.Verdict)));
                return sb.ToString();
            }

            Dictionary<string, Row> second = confirmation.ToDictionary(x => x.Key);
            sb.AppendLine("| Benchmark | Parameters | Job | Baseline (run 1) | Candidate (run 1) | Change run 1 | Change run 2 | Result |");
            sb.AppendLine("|---|---|---|---:|---:|---:|---:|---|");
            List<string> results = new();
            foreach (Row row in rows)
            {
                string result;
                string change2;
                if (second.TryGetValue(row.Key, out Row other) == false)
                {
                    result = "missing in run 2";
                    change2 = "-";
                }
                else
                {
                    change2 = other.Change.ToString("+0.0%;-0.0%;0.0%", CultureInfo.InvariantCulture);
                    result = row.Verdict == other.Verdict
                        ? row.Verdict == "same" ? "same" : $"confirmed {row.Verdict}"
                        : "inconsistent";
                }

                results.Add(result);
                sb.AppendLine(string.Create(CultureInfo.InvariantCulture,
                    $"| {row.Benchmark} | {row.Parameters} | {row.Job} | {FormatTime(row.Baseline.Mean)} | {FormatTime(row.Candidate.Mean)} | {row.Change:+0.0%;-0.0%;0.0%} | {change2} | {Bold(result)} |"));
            }

            sb.AppendLine();
            sb.AppendLine(string.Join(", ", results.GroupBy(x => x).OrderBy(x => x.Key).Select(x => $"{x.Key}: {x.Count()}")));
            return sb.ToString();
        }

        private static List<Row> CreateRows(string baselinePath, string candidatePath, ComparisonOptions options)
        {
            List<Result> baseline;
            List<Result> candidate;
            if (options.InRun)
            {
                // frozen pre-change code (X_Baseline) against production code (X); with job filters: old code on one job, new code on another
                baseline = Load(baselinePath).Where(x => x.Method.EndsWith(BaselineMethodSuffix, StringComparison.Ordinal)).ToList();
                candidate = Load(candidatePath).Where(x => x.Method.EndsWith(BaselineMethodSuffix, StringComparison.Ordinal) == false).ToList();
                foreach (Result result in baseline)
                    result.Method = result.Method[..^BaselineMethodSuffix.Length];
            }
            else
            {
                baseline = Load(baselinePath);
                candidate = Load(candidatePath);
            }

            if (options.BaselineJob != null)
                baseline = baseline.Where(x => x.Job == options.BaselineJob).ToList();
            if (options.CandidateJob != null)
                candidate = candidate.Where(x => x.Job == options.CandidateJob).ToList();

            // jobs are part of the key only when both sides hold several jobs that should be matched to each other
            bool matchJobs = options.BaselineJob == null && options.CandidateJob == null &&
                             baseline.Select(x => x.Job).Distinct().Count() > 1 && candidate.Select(x => x.Job).Distinct().Count() > 1;

            string Key(Result r) => $"{r.Type}.{r.Method}({r.Parameters})" + (matchJobs ? "@" + r.Job : string.Empty);

            Dictionary<string, Result> candidateByKey = candidate.GroupBy(Key).ToDictionary(x => x.Key, x => x.First());
            List<Row> rows = new();
            foreach (Result b in baseline.OrderBy(x => x.Type).ThenBy(x => x.Method).ThenBy(x => x.Parameters, StringComparer.Ordinal).ThenBy(x => x.Job, StringComparer.Ordinal))
            {
                if (candidateByKey.TryGetValue(Key(b), out Result c) == false)
                    continue;

                double change = c.Mean / b.Mean - 1;
                string verdict = c.Upper < b.Lower && change < -options.Threshold ? "faster"
                    : c.Lower > b.Upper && change > options.Threshold ? "slower"
                    : "same";

                rows.Add(new Row
                {
                    Key = Key(b),
                    Benchmark = $"{b.Type}.{b.Method}",
                    Parameters = b.Parameters.Replace("&", ", "),
                    Job = matchJobs || b.Job == c.Job ? b.Job : $"{b.Job} -> {c.Job}",
                    Baseline = b,
                    Candidate = c,
                    Change = change,
                    Verdict = verdict
                });
            }

            if (rows.Count == 0)
                throw new InvalidOperationException("Nothing to compare - check the paths, job ids and --in-run");

            return rows;
        }

        private static string Describe(string path, ComparisonOptions options, bool baseline)
        {
            string job = baseline ? options.BaselineJob : options.CandidateJob;
            string code = options.InRun ? (baseline ? ", frozen pre-change code (*_Baseline)" : ", production code") : string.Empty;
            return $"`{path}`{(job != null ? $", job {job}" : string.Empty)}{code}";
        }

        private static string Summary(IEnumerable<string> verdicts) =>
            string.Join(", ", verdicts.GroupBy(x => x).OrderBy(x => x.Key).Select(x => $"{x.Key}: {x.Count()}"));

        private static string Bold(string verdict) => verdict == "same" ? verdict : $"**{verdict}**";

        private static List<Result> Load(string path)
        {
            IEnumerable<string> files = Directory.Exists(path)
                ? Directory.EnumerateFiles(path, "*-report-full.json", SearchOption.AllDirectories)
                : new[] { path };

            List<Result> results = new();
            foreach (string file in files)
            {
                using JsonDocument document = JsonDocument.Parse(File.ReadAllBytes(file));
                foreach (JsonElement benchmark in document.RootElement.GetProperty("Benchmarks").EnumerateArray())
                {
                    if (benchmark.TryGetProperty("Statistics", out JsonElement statistics) == false || statistics.ValueKind != JsonValueKind.Object)
                        continue;

                    JsonElement interval = statistics.GetProperty("ConfidenceInterval");
                    double? allocated = null;
                    if (benchmark.TryGetProperty("Memory", out JsonElement memory) && memory.ValueKind == JsonValueKind.Object &&
                        memory.TryGetProperty("BytesAllocatedPerOperation", out JsonElement bytes) && bytes.ValueKind == JsonValueKind.Number)
                        allocated = bytes.GetDouble();

                    results.Add(new Result
                    {
                        Type = benchmark.GetProperty("Type").GetString(),
                        Method = benchmark.GetProperty("Method").GetString(),
                        Parameters = benchmark.GetProperty("Parameters").GetString() ?? string.Empty,
                        Job = ParseJob(benchmark.GetProperty("DisplayInfo").GetString()),
                        Mean = statistics.GetProperty("Mean").GetDouble(),
                        Lower = interval.GetProperty("Lower").GetDouble(),
                        Upper = interval.GetProperty("Upper").GetDouble(),
                        Allocated = allocated
                    });
                }
            }

            if (results.Count == 0)
                throw new InvalidOperationException($"No BenchmarkDotNet full JSON results found in {path}");

            return results;
        }

        // DisplayInfo looks like "Type.Method: JobId(Characteristic=..., ...) [Param=...]"
        private static string ParseJob(string displayInfo)
        {
            int start = displayInfo.IndexOf(": ", StringComparison.Ordinal);
            if (start < 0)
                return string.Empty;
            start += 2;
            int end = displayInfo.IndexOfAny(new[] { '(', ' ', '[' }, start);
            return (end < 0 ? displayInfo[start..] : displayInfo[start..end]).Trim();
        }

        private static string FormatTime(double nanoseconds)
        {
            if (nanoseconds >= 1_000_000_000)
                return (nanoseconds / 1_000_000_000).ToString("N3", CultureInfo.InvariantCulture) + " s";
            if (nanoseconds >= 1_000_000)
                return (nanoseconds / 1_000_000).ToString("N3", CultureInfo.InvariantCulture) + " ms";
            if (nanoseconds >= 1_000)
                return (nanoseconds / 1_000).ToString("N3", CultureInfo.InvariantCulture) + " us";
            return nanoseconds.ToString("N1", CultureInfo.InvariantCulture) + " ns";
        }

        private static string FormatBytes(double? bytes) => bytes == null ? "-" : bytes.Value.ToString("N0", CultureInfo.InvariantCulture) + " B";
    }
}

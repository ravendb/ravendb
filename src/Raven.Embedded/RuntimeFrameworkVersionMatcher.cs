using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Embedded
{
    internal static class RuntimeFrameworkVersionMatcher
    {
        internal const string Wildcard = "x";

        internal static async Task<string> MatchAsync(ServerOptions options)
        {
            if (NeedsMatch(options) == false)
                return options?.FrameworkVersion;

            var runtime = new RuntimeFrameworkVersion(options.FrameworkVersion);
            var runtimes = await GetFrameworkVersionsAsync(options).ConfigureAwait(false);

            return Match(runtime, runtimes);
        }

        internal static string Match(RuntimeFrameworkVersion runtime, List<RuntimeFrameworkVersion> runtimes)
        {
            var sortedRuntimes = runtimes
                .OrderByDescending(x => x.Major)
                .ThenByDescending(x => x.Minor)
                .ThenByDescending(x => x.Patch)
                .ToList();

            foreach (var version in sortedRuntimes)
            {
                if (runtime.Match(version))
                    return version.ToString();
            }

            var sb = new StringBuilder();
            sb.AppendLine($"Could not find a matching runtime for '{runtime}'. Available runtimes:");
            foreach (var r in sortedRuntimes)
                sb.AppendLine($"- {r}");

            throw new InvalidOperationException(sb.ToString());
        }

        private static bool NeedsMatch(ServerOptions options)
        {
            if (options == null || string.IsNullOrWhiteSpace(options.FrameworkVersion))
                return false;

            var frameworkVersionAsString = options.FrameworkVersion.ToLowerInvariant();
            if (frameworkVersionAsString.Contains(Wildcard) == false) // no wildcards
                return false;

            return true;
        }

        private static async Task<List<RuntimeFrameworkVersion>> GetFrameworkVersionsAsync(ServerOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.DotNetPath))
                throw new InvalidOperationException();

            var processStartInfo = new ProcessStartInfo
            {
                FileName = options.DotNetPath,
                Arguments = "--info",
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true,
                UseShellExecute = false
            };

            Process process = null;
            try
            {
                process = Process.Start(processStartInfo);
                process.EnableRaisingEvents = true;
            }
            catch (Exception e)
            {
                process?.Kill();
                throw new InvalidOperationException($"Unable to execute dotnet to retrieve list of installed runtimes.{Environment.NewLine}Command was: {Environment.NewLine}{processStartInfo.WorkingDirectory}> {processStartInfo.FileName} {processStartInfo.Arguments}", e);
            }

            var insideRuntimes = false;
            var runtimeLines = new List<string>();
            await ProcessHelper.ReadOutput(process.StandardOutput, elapsed: null, options, (line, builder) =>
            {
                line = line.Trim();

                if (line.StartsWith(".NET runtimes installed:") || line.StartsWith(".NET Core runtimes installed:"))
                {
                    insideRuntimes = true;
                    return Task.FromResult(false);
                }

                if (insideRuntimes && line.StartsWith("Microsoft.NETCore.App"))
                    runtimeLines.Add(line);

                return Task.FromResult(false);
            }).ConfigureAwait(false);

            var runtimes = new List<RuntimeFrameworkVersion>();
            foreach (string runtimeLine in runtimeLines) // Microsoft.NETCore.App 5.0.2 [C:\Program Files\dotnet\shared\Microsoft.NETCore.App]
            {
                var values = runtimeLine.Split(' ');
                if (values.Length < 2)
                    throw new InvalidOperationException($"Invalid runtime line. Expected 'Microsoft.NETCore.App x.x.x', but was '{runtimeLine}'.");

                runtimes.Add(new RuntimeFrameworkVersion(values[1]));
            }

            return runtimes;
        }

        internal class RuntimeFrameworkVersion
        {
            private static readonly char[] Separators = { '.' };

            private static readonly string SuffixSeparator = "-";

            public int? Major { get; private set; }

            public int? Minor { get; private set; }

            public int? Patch { get; internal set; }

            public string Suffix { get; set; }

            public RuntimeFrameworkVersion(string frameworkVersion)
            {
                frameworkVersion = frameworkVersion.ToLowerInvariant();
                var suffixes = frameworkVersion.Split(new[] { SuffixSeparator }, StringSplitOptions.RemoveEmptyEntries);
                if (suffixes.Length != 1)
                {
                    frameworkVersion = suffixes[0];
                    Suffix = string.Join(SuffixSeparator, suffixes.Skip(1));
                }

                var versions = frameworkVersion.Split(Separators, StringSplitOptions.RemoveEmptyEntries);
                for (var i = 0; i < versions.Length; i++)
                {
                    var version = versions[i].Trim();
                    if (version.Contains(Wildcard) == false)
                    {
                        var versionAsInt = Parse(version);
                        Set(i, versionAsInt);
                        continue;
                    }

                    if (version != Wildcard)
                        throw new InvalidOperationException($"Wildcard character must be a sole part of the version string, but was '{version}'."); // e.g. 3x, x7, etc

                    Set(i, value: null);
                }
            }

            public override string ToString()
            {
                var version = $"{Major?.ToString() ?? Wildcard}.{Minor?.ToString() ?? Wildcard}.{Patch?.ToString() ?? Wildcard}";
                if (Suffix != null)
                    version = $"{version}{SuffixSeparator}{Suffix}";

                return version;
            }

            private static int Parse(string value)
            {
                if (int.TryParse(value, out var valueAsInt) == false)
                    throw new InvalidOperationException($"Cannot parse '{value}' to a number.");

                return valueAsInt;
            }

            private void Set(int i, int? value)
            {
                switch (i)
                {
                    case 0:
                        Major = value;
                        break;
                    case 1:
                        Minor = value;
                        break;
                    case 2:
                        Patch = value;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(i));
                }
            }

            public bool Match(RuntimeFrameworkVersion version)
            {
                if (Major.HasValue && Major != version.Major)
                    return false;

                if (Minor.HasValue && Minor != version.Minor)
                    return false;

                if (Patch.HasValue && Patch != version.Patch)
                    return false;

                if (Suffix != version.Suffix)
                    return false;

                return true;
            }
        }
    }
}

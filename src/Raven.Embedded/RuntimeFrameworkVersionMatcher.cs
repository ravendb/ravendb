using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Embedded
{
    internal static class RuntimeFrameworkVersionMatcher
    {
        internal const string Wildcard = "x";

        internal const char GreaterOrEqual = '+';

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
            if (frameworkVersionAsString.Contains(Wildcard) == false && frameworkVersionAsString.Contains(GreaterOrEqual) == false) // no wildcards && no greaterOrEqual
                return false;

            return true;
        }

        private static async Task<List<RuntimeFrameworkVersion>> GetFrameworkVersionsAsync(ServerOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.DotNetPath))
                throw new InvalidOperationException("'ServerOptions.DotNetPath' must specify the dotnet executable used to discover installed .NET runtimes.");

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

            using var process = new Process();
            process.StartInfo = processStartInfo;
            process.EnableRaisingEvents = true;

            var workingDirectory = Directory.GetCurrentDirectory();
            try
            {
                process.Start();
            }
            catch (Exception e)
            {
                string message = $"Unable to execute dotnet to retrieve list of installed runtimes.{Environment.NewLine}" +
                                 $"Command was: {Environment.NewLine}" +
                                 $"{workingDirectory}> \"{processStartInfo.FileName}\" {processStartInfo.Arguments}";

                throw new InvalidOperationException(message, e);
            }

            var isInsideRuntimes = false;
            var runtimeLines = new List<string>();
            var output = process.ReadOutput(onOutputLine: line =>
            {
                line = line.Trim();
                if (line.StartsWith(".NET runtimes installed:") || line.StartsWith(".NET Core runtimes installed:"))
                    isInsideRuntimes = true;
                else if (isInsideRuntimes && line.StartsWith("Microsoft.NETCore.App"))
                    runtimeLines.Add(line);
            });

            try
            {
                process.StandardInput.Close();

                if (await output.Completion.WaitWithTimeout(options.MaxServerStartupTimeDuration).ConfigureAwait(false) == false)
                    throw new InvalidOperationException($"The dotnet runtime discovery command did not complete within {options.MaxServerStartupTimeDuration}.");

                await output.Completion.ConfigureAwait(false);
                if (process.ExitCode != 0)
                    throw new InvalidOperationException("The dotnet runtime discovery command failed.");

                var runtimes = new List<RuntimeFrameworkVersion>();
                foreach (string line in runtimeLines)
                {
                    var values = line.Split(' ');
                    if (values.Length < 2)
                        throw new InvalidOperationException($"Invalid runtime line. Expected 'Microsoft.NETCore.App x.x.x', but was '{line}'.");

                    runtimes.Add(new RuntimeFrameworkVersion(values[1]));
                }

                return runtimes.Count != 0
                    ? runtimes
                    : throw new InvalidOperationException("The command completed successfully, but no Microsoft.NETCore.App runtimes were found in its output. " +
                                                          "Check the .NET installation and the output below.");
            }
            catch (Exception e)
            {
                var exitCodeBeforeTermination = output.ExitCode;
                try
                {
                    if (process.HasExited == false)
                        process.Kill();
                }
                catch (Exception cleanupError) when (cleanupError is Win32Exception or InvalidOperationException)
                {
                    // Cleanup must not replace the original discovery failure.
                }

                await output.DrainAsync(options.ProcessKillTimeout).ConfigureAwait(false);

                var message = new StringBuilder();
                message.AppendLine("Unable to discover installed .NET runtimes.");
                message.AppendLine(e.Message);
                message.AppendLine($"Command: \"{processStartInfo.FileName}\" {processStartInfo.Arguments}");
                message.AppendLine("Run the command above from the same working directory and under the same account/environment as the tests. Check ServerOptions.DotNetPath and the .NET installation.");

                output.AppendDiagnostics(message, exitCodeBeforeTermination);
                throw new InvalidOperationException(message.ToString(), e);
            }
        }
        internal sealed class RuntimeFrameworkVersion
        {
            private static readonly char[] Separators = { '.' };

            private static readonly string SuffixSeparator = "-";

            public int? Major { get; private set; }

            public int? Minor { get; private set; }

            public int? Patch { get; internal set; }

            public MatchingType PatchMatchingType { get; internal set; }

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
                        var (versionAsInt, matchingType) = Parse(version);
                        Set(i, version, versionAsInt, matchingType);
                        continue;
                    }

                    if (version != Wildcard)
                        throw new InvalidOperationException($"Wildcard character must be a sole part of the version string, but was '{version}'."); // e.g. 3x, x7, etc

                    Set(i, valueAsString: null, value: null, matchingType: MatchingType.Equal);
                }
            }

            public override string ToString()
            {
                var version = $"{ToStringInternal(Major, MatchingType.Equal)}.{ToStringInternal(Minor, MatchingType.Equal)}.{ToStringInternal(Patch, PatchMatchingType)}";

                if (Suffix != null)
                    version = $"{version}{SuffixSeparator}{Suffix}";

                return version;

                static string ToStringInternal(int? number, MatchingType matchingType)
                {
                    if (number.HasValue == false)
                        return Wildcard;

                    switch (matchingType)
                    {
                        case MatchingType.Equal:
                            return number.ToString();
                        case MatchingType.GreaterOrEqual:
                            return $"{number}{GreaterOrEqual}";
                        default:
                            throw new ArgumentOutOfRangeException(nameof(matchingType), matchingType, null);
                    }
                }
            }

            private static (int Value, MatchingType MatchingType) Parse(string value)
            {
                var matchingType = MatchingType.Equal;

                var valueToParse = value;

                var lastChar = valueToParse[valueToParse.Length - 1];
                if (lastChar == GreaterOrEqual)
                {
                    matchingType = MatchingType.GreaterOrEqual;
                    valueToParse = valueToParse.Substring(0, valueToParse.Length - 1);
                }

                if (int.TryParse(valueToParse, out int valueAsInt) == false)
                    throw new InvalidOperationException($"Cannot parse '{value}' to a number.");

                return (valueAsInt, matchingType);
            }

            private void Set(int i, string valueAsString, int? value, MatchingType matchingType)
            {
                switch (i)
                {
                    case 0:
                        AssertMatchingType(nameof(Major), valueAsString, MatchingType.Equal, matchingType);
                        Major = value;
                        break;
                    case 1:
                        AssertMatchingType(nameof(Minor), valueAsString, MatchingType.Equal, matchingType);
                        Minor = value;
                        break;
                    case 2:
                        AssertMatchingType(nameof(Patch), valueAsString, expectedMatchingType: null, matchingType);
                        Patch = value;
                        PatchMatchingType = matchingType;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(i));
                }

                void AssertMatchingType(string fieldName, string valueAsString, MatchingType? expectedMatchingType, MatchingType matchingType)
                {
                    if (Suffix != null && matchingType != MatchingType.Equal)
                        throw new InvalidOperationException($"Cannot set '{fieldName}' with value '{valueAsString}' because '{MatchingTypeToString(matchingType)}' is not allowed when Suffix ('{Suffix}') is set.");

                    if (expectedMatchingType.HasValue && expectedMatchingType != matchingType)
                        throw new InvalidOperationException($"Cannot set '{fieldName}' with value '{valueAsString}' because '{MatchingTypeToString(matchingType)}' is not allowed.");
                }

                static string MatchingTypeToString(MatchingType matchingType)
                {
                    switch (matchingType)
                    {
                        case MatchingType.Equal:
                            return string.Empty;
                        case MatchingType.GreaterOrEqual:
                            return GreaterOrEqual.ToString();
                        default:
                            throw new ArgumentOutOfRangeException(nameof(matchingType), matchingType, null);
                    }
                }
            }

            public bool Match(RuntimeFrameworkVersion version)
            {
                if (Major.HasValue && Major != version.Major)
                    return false;

                if (Minor.HasValue && Minor != version.Minor)
                    return false;

                if (Patch.HasValue)
                {
                    switch (PatchMatchingType)
                    {
                        case MatchingType.Equal:
                            if (Patch != version.Patch)
                                return false;
                            break;
                        case MatchingType.GreaterOrEqual:
                            if (Patch > version.Patch)
                                return false;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                if (Suffix != version.Suffix)
                    return false;

                return true;
            }
        }

        internal enum MatchingType
        {
            Equal,
            GreaterOrEqual
        }
    }
}

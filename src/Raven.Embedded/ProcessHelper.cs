using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Embedded
{
    internal static class ProcessHelper
    {
        internal static async Task<string?> ReadOutput(StreamReader output, Stopwatch elapsed, ServerOptions options, Func<string, StringBuilder, Task<bool>>? onLine)
        {
            var sb = new StringBuilder();

            Task<string>? readLineTask = null;
            while (true)
            {
                readLineTask ??= output.ReadLineAsync();

                var hasResult = await readLineTask.WaitWithTimeout(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                if (elapsed != null && elapsed.Elapsed > options.MaxServerStartupTimeDuration)
                    return null;

                if (hasResult == false)
                    continue;

                var line = readLineTask.Result;

                readLineTask = null;

                var shouldStop = false;
                if (line != null)
                {
                    sb.AppendLine(line);

                    if (onLine != null)
                        shouldStop = await onLine(line, sb).ConfigureAwait(false);
                }

                if (shouldStop)
                    break;

                if (line == null)
                    break;
            }

            return sb.ToString();
        }

    }
}

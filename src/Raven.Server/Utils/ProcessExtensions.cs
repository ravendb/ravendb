using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Server.Utils
{
    public static class ProcessExtensions
    {
        public static bool TryKill(Process process)
        {
            try
            {
                process?.Kill();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static bool TryClose(Process process)
        {
            try
            {
                process?.Close();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static async Task<string> ReadOutput(StreamReader output)
        {
            var sb = new StringBuilder();

            Task<string> readLineTask = null;
            while (true)
            {
                if (readLineTask == null)
                    readLineTask = output.ReadLineAsync();

                var hasResult = await readLineTask.WaitWithTimeout(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                if (hasResult == false)
                    continue;

                var line = readLineTask.Result;

                readLineTask = null;

                if (line != null)
                    sb.AppendLine(line);

                if (line == null)
                    break;
            }
            return sb.ToString();
        }
    }
}

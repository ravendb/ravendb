using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Raven.Tests.Util
{
    public abstract class ProcessDriver
    {
        protected Process _process;

        protected void StartProcess(string exePath, string arguments = "")
        {
            ProcessStartInfo psi = new ProcessStartInfo(exePath);
                
            psi.LoadUserProfile = false;

            psi.Arguments = arguments;
            psi.UseShellExecute = false;
            psi.RedirectStandardError = true;
            psi.RedirectStandardInput = true;
            psi.RedirectStandardOutput = true;
            psi.CreateNoWindow = true;

            _process = Process.Start(psi);
        }

        protected virtual void Shutdown() { }

        public void Dispose()
        {
            if (_process != null)
            {
                Shutdown();

                var toDispose = _process;
                _process = null;

                toDispose.Dispose();
            }
        }

        protected Match WaitForConsoleOutputMatching(string pattern)
        {
            Match match;
            while(true)
            {
                var nextLine = _process.StandardOutput.ReadLine();
                
                match = Regex.Match(nextLine, pattern);

                if (match.Success)
                    break;
            }
            return match;
        }
    }
}
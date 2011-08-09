using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Threading;

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

        protected Match WaitForConsoleOutputMatching(string pattern, int msMaxWait = 10000, int msWaitInterval = 500)
        {
            int totalWaited = 0;

            Match match;
            while(true)
            {
                var nextLine = _process.StandardOutput.ReadLine();

                if (nextLine == null)
                {
                    if (totalWaited > msMaxWait)
                        throw new TimeoutException("Timeout waiting for regular expression " + pattern);
                    
                    Thread.Sleep(msWaitInterval);
                    totalWaited += msWaitInterval;
                    continue;
                }
                
                match = Regex.Match(nextLine, pattern);

                if (match.Success)
                    break;
            }
            return match;
        }
    }
}
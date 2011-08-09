using System.Diagnostics;

namespace Raven.Tests.Util
{
    public abstract class ProcessDriver
    {
        protected Process _process;

        public void StartProcess(string exePath)
        {
            ProcessStartInfo psi = new ProcessStartInfo(exePath);
                
            psi.LoadUserProfile = false;

            psi.UseShellExecute = false;
            psi.RedirectStandardError = true;
            psi.RedirectStandardInput = true;
            psi.RedirectStandardOutput = true;
            psi.CreateNoWindow = true;

            _process = Process.Start(psi);
        }

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

        protected abstract void Shutdown();
    }
}
using System;
using Custom.Raven.System.Diagnostics;
using Sparrow.Platform;

namespace Raven.Server.Utils.Cli
{
    public static class BrowserHelper
    {
        public static bool OpenStudioInBrowser(string url, Action<object> onError = null)
        {
            try
            {
                if (PlatformDetails.RunningOnPosix == false)
                {
                    RavenProcess.Start(new ProcessStartInfo("cmd", $"/c start {url}") { InheritHandles = false });
                }
                else if (PlatformDetails.RunningOnMacOsx)
                {
                    System.Diagnostics.Process.Start("open", url);
                }
                else
                {
                    System.Diagnostics.Process.Start("xdg-open", url);
                }

                return true;
            }
            catch (Exception e)
            {
                var error = $"Could not start browser: {e}";
                if (onError != null)
                {
                    onError.Invoke(error);
                }
                else
                {
                    Console.WriteLine(error);
                }

                return false;
            }
        }
    }
}

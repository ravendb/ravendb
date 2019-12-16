using System;
using System.Diagnostics;
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
                    Process.Start(new ProcessStartInfo("cmd", $"/c start {url}"));
                }
                else if (PlatformDetails.RunningOnMacOsx)
                {
                    Process.Start("open", url);
                }
                else
                {
                    Process.Start("xdg-open", url);
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

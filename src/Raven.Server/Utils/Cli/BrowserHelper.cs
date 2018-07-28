using System;
using System.Diagnostics;
using Sparrow.Platform;

namespace Raven.Server.Utils.Cli
{
    public static class BrowserHelper
    {
        public static bool OpenStudioInBrowser(string url, out string error)
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

                error = null;
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not start browser: " + e.Message);
                error = e.Message;
                return false;
            }
        }
    }
}

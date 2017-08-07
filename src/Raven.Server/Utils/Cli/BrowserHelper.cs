using System;
using System.Diagnostics;
using Sparrow.Platform;

namespace Raven.Server.Utils.Cli
{
    public static class BrowserHelper
    {
        public static void OpenStudioInBrowser(string url)
        {
            try
            {
                if (PlatformDetails.RunningOnPosix == false)
                {
                    Process.Start(new ProcessStartInfo("cmd", $"/c start {url}"));
                }
                else
                {
                    Process.Start("xdg-open", url);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not start browser: " + e.Message);
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Server.Config;
using Sparrow.Platform;
using Voron.Platform.Posix;

namespace Raven.Server.Utils
{
    public static class BrowserHelper
    {
        public static void OpenStudioInBrowser(RavenServer server)
        {
            try
            {
                var url = server.WebUrls.First().Replace("0.0.0.0", "localhost");
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

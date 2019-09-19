using System;
using System.Diagnostics;
using System.Threading;
using Raven.Server.ServerWide;
using Sparrow.Platform;
using Sparrow.Server.Platform;

namespace Raven.Server.Utils.Cli
{
    public static class BrowserHelper
    {
        public static bool OpenStudioInBrowser(ServerStore serverStore, Action<object> onError = null)
        {
            var url = serverStore.GetNodeHttpServerUrl();
            try
            {
                if (PlatformDetails.RunningOnPosix == false)
                {
                    RavenProcess.Start("cmd", $"/c start {url}");
                }
                else if (PlatformDetails.RunningOnMacOsx)
                {
                    RavenProcess.Start("open", url);
                }
                else
                {
                    RavenProcess.Start("xdg-open", url);
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

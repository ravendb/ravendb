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
                    // ADIADI::RavenProcess.Start("xdg-open", url);
//                    using (var p = new RavenProcess {StartInfo = new ProcessStartInfo{FileName = "ls", Arguments = "-la"}})
//                    {
                    int i = 0;
                    var cts1 = new CancellationTokenSource();
                    var ctk1 = cts1.Token;
                    var cts2 = new CancellationTokenSource();
                    var ctk2 = cts2.Token;
                    var cts3 = new CancellationTokenSource();
                    var ctk3 = cts3.Token;
                    var cts4 = new CancellationTokenSource();
                    var ctk4 = cts4.Token;
                        EventHandler lineOutHandler = (sender, args) =>
                        {
                            LineOutputEventArgs l = args as LineOutputEventArgs;
                            Console.WriteLine(i + " - " + l.line);
                            i++;
//                            if (i > 13)
//                            {
//                                Console.WriteLine("About to cancel");
//                                cts4.Cancel();
//                            }
                        };

                        EventHandler exitHandler = (sender, args) =>
                        {
                            ProcessExitedEventArgs l = args as ProcessExitedEventArgs;
                            Console.WriteLine("exit:");
                            Console.WriteLine(l.ExitCode + ", " + l.Pid.ToInt64());
                            Console.WriteLine("===");
                        };

                        RavenProcess.Execute("testme.sh", "Second", 1, exitHandler, lineOutHandler, serverStore.ServerShutdown);

//                    }

                    Console.WriteLine("After");
                    // RavenProcess.Start("testme.sh", url);
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

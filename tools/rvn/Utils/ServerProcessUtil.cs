using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace rvn.Utils
{
    public class ServerProcessUtil
    {
        public static bool IsServerDown(int pid)
        {
            var process = Process.GetProcessById(pid);
            return process == null || process.HasExited;
        }

        public static int GetRavenServerPid()
        {
            var processes = Process.GetProcessesByName("Raven.Server");
            var thisProcess = Process.GetCurrentProcess();
            var availableRavenProcesses = new List<Process>();
            foreach (var pr in processes)
            {
                if (thisProcess.Id == pr.Id)
                    continue;
                availableRavenProcesses.Add(pr);
            }

            if (availableRavenProcesses.Count == 0)
            {
                Console.WriteLine("Couldn't find automatically another Raven.Server process." + Environment.NewLine +
                                  "Please specify RavenDB Server proccess manually" + Environment.NewLine);
                Console.Out.Flush();
                Environment.Exit(1);
            }
            else if (availableRavenProcesses.Count == 1)
            {
                Console.WriteLine("Will try to connect to discovered Raven.Server process : " + availableRavenProcesses.First().Id + "..." + Environment.NewLine);
                Console.Out.Flush();
                return availableRavenProcesses.First().Id;
            }

            Console.Write("More then one Raven.Server process where found:");
            availableRavenProcesses.ForEach(x => Console.Write(" " + x.Id));
            Console.WriteLine(Environment.NewLine + "Please specify RavenDB Server proccess manually" + Environment.NewLine);
            Console.Out.Flush();
            Environment.Exit(2);
            return 0;
        }
    }
}

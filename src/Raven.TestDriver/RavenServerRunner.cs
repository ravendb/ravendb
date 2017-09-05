using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Raven.TestDriver
{
    internal class RavenServerRunner<TLocator> where TLocator : RavenServerLocator
    {
        public static Process Run(TLocator locator)
        {
            var processStartInfo = GetProcessStartInfo(locator);
            Process result = null;
            try
            {
                result = Process.Start((ProcessStartInfo)processStartInfo);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                result?.Kill();
                throw;
            }

            return result;
        }

        private static ProcessStartInfo GetProcessStartInfo(TLocator locator)
        {
            if (File.Exists(locator.ServerPath) == false)
            {
                throw new FileNotFoundException("Server file was not found", locator.ServerPath);
            }

            var commandArguments = new List<string>
            {
                    locator.CommandArguments,
                    "--ServerUrl=http://127.0.0.1:0",
                    "--RunInMemory=true",
                    "--Testing.ParentProcessId=" + Process.GetCurrentProcess().Id
                };

            var argumentsString = string.Join(" ", commandArguments); 
            return new ProcessStartInfo()
            {
                FileName = locator.Command,
                Arguments = argumentsString,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
        }
    }
}

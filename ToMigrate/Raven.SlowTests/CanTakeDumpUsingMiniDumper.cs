using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Tests.Common;
using Raven.Tests.Common.Attributes;
using System.Web;
using Jint.Parser.Ast;
using Xunit;

namespace Raven.SlowTests
{
    public class CanTakeDumpUsingMiniDumper: RavenTest
    {
        [RavenServerAndMiniDumperAvailable]
        public void CanTakeADump()
        {
            var tmpPath = Path.GetTempPath();
            var tmpFile = Path.Combine(tmpPath, $"{Guid.NewGuid()}.tmp");
            var tmpDir = Path.Combine(Path.GetTempPath(), "RavenDBDumps");
            Process ravenProcess = null;
            try
            {
                File.Move(RavenServerAndMiniDumperAvailable.LocalConfigPath, tmpFile);
                File.WriteAllText(RavenServerAndMiniDumperAvailable.LocalConfigPath, WebUtility.HtmlDecode(localConfig));                
                CreateRavenProcess(out ravenProcess);
                var miniDumper = Process.Start(RavenServerAndMiniDumperAvailable.MiniDumperPath);//, $"--process-id={ravenProcess.Id}");
                WaitForMiniDumpProcessToExit(miniDumper);
                var dumps = Directory.EnumerateFiles(tmpDir, "*.dmp");
                Assert.NotEmpty(dumps);
                //this will fail if an old size 0 dump is located at the raven tmp dump folder
                Assert.True(dumps.Select(dump=>new FileInfo(dump)).All(file=>file.Length>0));
            }
            finally
            {
                File.Delete(RavenServerAndMiniDumperAvailable.LocalConfigPath);
                File.Move(tmpFile, RavenServerAndMiniDumperAvailable.LocalConfigPath);
                if (ravenProcess != null)
                {
                    ravenProcess.StandardInput.WriteLine("q");
                }
                Directory.Delete(tmpDir,true);
            }
        }

        private void WaitForMiniDumpProcessToExit(Process miniDumper)
        {
            var sw = new Stopwatch();
            sw.Start();
            while (sw.ElapsedMilliseconds < 10*1000)
            {
                if (miniDumper.HasExited)
                {
                    if (miniDumper.ExitCode != 0)
                    {
                        throw new Exception("Failed to generate minidump");
                    }
                    return;
                }
            }
            miniDumper.Kill();
            throw new TimeoutException("Waited for 10 seconds for minidump process to exit, giving up");
            
        }

        private void CreateRavenProcess(out Process ravenProcess)
        {
            var startInfo = new ProcessStartInfo();
            startInfo.UseShellExecute = false;
            startInfo.RedirectStandardInput = true;
            startInfo.FileName = RavenServerAndMiniDumperAvailable.RavenServerPath;
            ravenProcess = new Process();
            ravenProcess.StartInfo = startInfo;
            ravenProcess.Start();
            WaitForServerToLoad();
        }

        private void WaitForServerToLoad()
        {
            var sw = new Stopwatch();
            sw.Start();
            while (sw.ElapsedMilliseconds < 20*1000)
            {
                try
                {
                    var request = WebRequest.Create("http://localhost:8731/build/version");
                    request.Method = "GET";
                    WebResponse response = request.GetResponse();
                    if (response != null)
                        return;
                }
                catch 
                {
                    
                }

            }
            throw new TimeoutException("Waited 20 seconds for raver server to start, giving up.");
        }

        private const string localConfig = "&lt;?xml version = &quot;1.0&quot; encoding=&quot;utf-8&quot;?&gt;\n&lt;LocalConfig Port = &quot;8731&quot; /&gt;";
    }
}

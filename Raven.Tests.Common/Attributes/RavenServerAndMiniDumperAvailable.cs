using System;
using System.Collections.Generic;
using System.IO;

using Xunit;
using Xunit.Sdk;


namespace Raven.Tests.Common.Attributes
{

    [CLSCompliant(false)]
    public class RavenServerAndMiniDumperAvailable : FactAttribute
    {
        protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
        {
            var displayName = method.TypeName + "." + method.Name;

            if (LookForRavenPaths() == false)
            {
                yield return
                    new SkipCommand(method, displayName,
                        "Could not execute " + displayName + " because it requires Raven.Server.exe and Raven.MiniDumper.exe");
                yield break;
            }

            foreach (var command in base.EnumerateTestCommands(method))
            {
                yield return command;
            }
        }

        private bool LookForRavenPaths()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var expectedRavenPath = Path.Combine(baseDir, @"..\..\..\Raven.Server\bin\Debug\Raven.Server.exe");
            if (File.Exists(expectedRavenPath))
            {
                RavenServerPath = expectedRavenPath;
            }
            else
            {
                expectedRavenPath = Path.Combine(baseDir, @"..\..\..\Raven.Server\bin\Release\Raven.Server.exe");
                if (File.Exists(expectedRavenPath))
                    RavenServerPath = expectedRavenPath;
                else return false;
            }
            var expectedMiniDumperPath = Path.Combine(baseDir, @"..\..\..\Raven.MiniDumper\bin\Debug\Raven.MiniDumper.exe");
            if (File.Exists(expectedMiniDumperPath))
                MiniDumperPath = expectedMiniDumperPath;
            else
            {
                expectedMiniDumperPath = Path.Combine(baseDir, @"..\..\..\Raven.MiniDumper\bin\Release\Raven.MiniDumper.exe");
                if (File.Exists(expectedMiniDumperPath))
                    MiniDumperPath = expectedMiniDumperPath;
                else return false;
            }
            var ravenBaseDir = Path.GetDirectoryName(expectedRavenPath);
            LocalConfigPath = Path.Combine(ravenBaseDir, "local.config");
            //should not happen
            if (!File.Exists(LocalConfigPath))
                return false;
            return true;
        }

        public static string RavenServerPath { get; set; }
        public static string MiniDumperPath { get; set; }
        public static string LocalConfigPath { get; set; }
    }
    
}

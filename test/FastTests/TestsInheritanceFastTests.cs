using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Tests.Infrastructure;
using Voron.Util.Settings;
using Xunit;
using Xunit.Abstractions;

namespace FastTests
{
    public class TestsInheritanceFastTests : RavenLowLevelTestBase
    {
        public TestsInheritanceFastTests(ITestOutputHelper output) : base(output)
        {
        }

        [NonLinuxFact]
        public void AllTestsShouldUseRavenFactOrRavenTheoryAttributes()
        {
            var assemblies = new HashSet<Assembly>();
            string projectPath = FindSlowTestsDirectory();
            string outputDir = NewDataPath(suffix: "BuildOutput", forceCreateDir: true); // Temporary directory

            var projectCsProj = PathUtil.ToFullPath(Path.Combine(projectPath, "SlowTests.csproj"));

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "dotnet",
                    Arguments = $"publish \"{projectCsProj}\" --configuration Debug --output \"{outputDir}\" --no-restore",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };

            process.Start();
            string output = process.StandardOutput.ReadToEnd();
            string error = process.StandardError.ReadToEnd();
            var exited = process.WaitForExit(TimeSpan.FromMinutes(5));
            Assert.True(exited, "Process timeout!");
            Assert.True(process.ExitCode == 0, $"Build failed! Output:{Environment.NewLine}{output}{Environment.NewLine}Error:{Environment.NewLine}{error}");

            var outputDll = Directory.GetDirectories(PathUtil.ToFullPath(Path.Combine(projectPath, "bin", "Debug"))).FirstOrDefault();
            Assert.NotNull(outputDll);

            outputDll = PathUtil.ToFullPath(Path.Combine(outputDll, "SlowTests.dll"));
            Assert.True(File.Exists(outputDll), $"DLL not found: {outputDll}");

            var loaded = Assembly.LoadFrom(outputDll);
            var types = from assembly in GetAssemblies(assemblies, loaded)
                from test in GetAssemblyTypes(assembly)
                from method in test.GetMethods()
                where Filter(method)
                select method;

            var array = types.ToArray();
            const int numberToTolerate = 6346;
            if (array.Length == numberToTolerate)
                return;

            var userMessage = $"We have detected '{array.Length}' test(s) that do not have {nameof(RavenFactAttribute)} or {nameof(RavenTheoryAttribute)} attribute. Please check if tests that you have added have those attributes. List of test files:{Environment.NewLine}{string.Join(Environment.NewLine, array.Select(x => GetTestName(x)))}";
            throw new Exception(userMessage);

            static string GetTestName(MethodInfo method)
            {
                return $"{method.DeclaringType?.FullName}.{method.Name}";
            }

            static bool Filter(MethodInfo method)
            {
                var factAttribute = method.GetCustomAttribute(typeof(FactAttribute), false);
                if (factAttribute != null)
                {
                    if (ValidNamespace(factAttribute.GetType().Namespace))
                        return false;

                    return true;
                }

                var theoryAttribute = method.GetCustomAttribute(typeof(TheoryAttribute), false);
                if (theoryAttribute != null)
                {
                    if (ValidNamespace(theoryAttribute.GetType().Namespace))
                        return false;

                    return true;
                }

                return false;
            }

            static bool ValidNamespace(string @namespace)
            {
                return @namespace == null || @namespace.StartsWith("FastTests") || @namespace.StartsWith("SlowTests") || @namespace.StartsWith("Tests.Infrastructure");
            }
        }

        internal static IEnumerable<Assembly> GetAssemblies(HashSet<Assembly> assemblies, Assembly assemblyToScan)
        {
            if (assemblies.Add(assemblyToScan) == false)
                yield break;

            yield return assemblyToScan;

            foreach (var asm in assemblyToScan.GetReferencedAssemblies())
            {

                Assembly load;
                try
                {
                    load = Assembly.Load(asm);
                }
                catch
                {
                    continue;
                }
                foreach (var assembly in GetAssemblies(assemblies, load))
                    yield return assembly;
            }
        }

        internal static Type[] GetAssemblyTypes(Assembly assemblyToScan)
        {
            try
            {
                return assemblyToScan.GetTypes();
            }
            catch
            {
                return Array.Empty<Type>();
            }
        }

        private static string FindSlowTestsDirectory()
        {
            for (int i = 1; i < 10; i++)
            {
                var dir = string.Concat(Enumerable.Repeat("../", i)) + "test/SlowTests/";
                var fullPath = PathUtil.ToFullPath(Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, dir)));
                if (Directory.Exists(fullPath))
                    return fullPath;
            }

            throw new FileNotFoundException("Unable to find Studio directory");
        }
    }
}

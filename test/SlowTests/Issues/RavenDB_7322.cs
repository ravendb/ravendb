using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7322 : NoDisposalNeeded
    {
        public RavenDB_7322(ITestOutputHelper output) : base(output)
        {
        }

        private readonly HashSet<Assembly> _assemblies = new HashSet<Assembly>();

        // In linux we might encounter Microsoft's VisualStudio assembly types, so we skip this test in linux, and rely on the windows tests result as good for linux too
        [NonLinuxFact]
        public void TestClassesShouldNotInheritFromOtherTestClassesToNotMultiplyTests()
        {
            var classes = from assembly in GetAssemblies(typeof(RavenDB_7322).Assembly)
                          from test in GetAssemblyTypes(assembly)
                          where test.GetMethods().Any(x => x.GetCustomAttributes(typeof(FactAttribute), true).Count() != 0 || x.GetCustomAttributes(typeof(TheoryAttribute), true).Count() != 0)
                          select test;

            var dictionary = classes.ToDictionary(x => x, x => x.BaseType);

            var sb = new StringBuilder();
            foreach (var kvp in dictionary)
            {
                if (dictionary.TryGetValue(kvp.Value, out var _) == false)
                    continue;

                sb.Append($"Class '{kvp.Key.FullName}' inherits from '{kvp.Value.FullName}'");
            }

            if (sb.Length == 0)
                return;

            throw new InvalidOperationException($"Detected that some test classes are inheriting from each other.{Environment.NewLine}{sb}");
        }

        private IEnumerable<Assembly> GetAssemblies(Assembly assemblyToScan)
        {
            if (_assemblies.Add(assemblyToScan) == false)
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
                foreach (var assembly in GetAssemblies(load))
                    yield return assembly;
            }
        }

        private static Type[] GetAssemblyTypes(Assembly assemblyToScan)
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
    }
}

// -----------------------------------------------------------------------
//  <copyright file="NoNonDisposableTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Web;
using Raven.TestDriver;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests
{
    public class TestsInheritanceTests : NoDisposalNeeded
    {
        public TestsInheritanceTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly HashSet<Assembly> _assemblies = new HashSet<Assembly>();

        // In linux we might encounter Microsoft's VisualStudio assembly types, so we skip this test in linux, and rely on the windows tests result as good for linux too
        [NonLinuxFact]
        public void NonDisposableTestShouldNotExist()
        {
            var types = from assembly in GetAssemblies(typeof(TestsInheritanceTests).Assembly)
                        from test in GetAssemblyTypes(assembly)
                        where test.GetMethods().Any(x => x.GetCustomAttributes(typeof(FactAttribute), true).Count() != 0 || x.GetCustomAttributes(typeof(TheoryAttribute), true).Count() != 0)
                        where typeof(IDisposable).IsAssignableFrom(test) == false
                        select test;

            var array = types.ToArray();
            if (array.Length == 0)
                return;

            var userMessage = string.Join(Environment.NewLine, array.Select(x => x.FullName));
            throw new Exception(userMessage);
        }

        [NonLinuxFact]
        public void TestsShouldInheritFromRightBaseClasses()
        {
            var types = from assembly in GetAssemblies(typeof(TestsInheritanceTests).Assembly)
                        from test in GetAssemblyTypes(assembly)
                        where test.GetMethods().Any(x => x.GetCustomAttributes(typeof(FactAttribute), true).Count() != 0 || x.GetCustomAttributes(typeof(TheoryAttribute), true).Count() != 0)
                        where test.IsSubclassOf(typeof(ParallelTestBase)) == false && test.IsSubclassOf(typeof(RavenTestDriver)) == false && test.Namespace.StartsWith("EmbeddedTests") == false
                        select test;

            var array = types.ToArray();
            if (array.Length == 0)
                return;

            var userMessage = string.Join(Environment.NewLine, array.Select(x => x.FullName));
            throw new Exception(userMessage);
        }

        [NonLinuxFact]
        public void HandlersShouldNotInheritStraightFromRequestHandler()
        {
            var types = from assembly in GetAssemblies(typeof(TestsInheritanceTests).Assembly)
                        from handler in GetAssemblyTypes(assembly)
                        where handler.IsAbstract == false
                        where handler != typeof(DatabaseRequestHandler) && handler != typeof(ServerRequestHandler) && handler != typeof(ShardedDatabaseRequestHandler)
                        where handler.IsSubclassOf(typeof(RequestHandler)) && handler.IsSubclassOf(typeof(ServerRequestHandler)) == false && handler.IsSubclassOf(typeof(DatabaseRequestHandler)) == false && handler.IsSubclassOf(typeof(ShardedDatabaseRequestHandler)) == false
                        select handler;

            var array = types.ToArray();
            if (array.Length == 0)
                return;

            var userMessage = string.Join(Environment.NewLine, array.Select(x => x.FullName));
            throw new Exception(userMessage);
        }

        [Fact]
        public void AllTestsShouldUseRavenFactOrRavenTheoryAttributes()
        {
            var types = from assembly in GetAssemblies(typeof(TestsInheritanceTests).Assembly)
                        from test in GetAssemblyTypes(assembly)
                        from method in test.GetMethods()
                        where Filter(method)
                        select method;

            var array = types.ToArray();

            const int numberToTolerate = 4559;

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

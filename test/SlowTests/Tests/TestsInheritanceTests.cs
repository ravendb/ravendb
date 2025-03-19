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
            var types = from assembly in TestsInheritanceFastTests.GetAssemblies(_assemblies, typeof(TestsInheritanceTests).Assembly)
                        from test in TestsInheritanceFastTests.GetAssemblyTypes(assembly)
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
            var types = from assembly in TestsInheritanceFastTests.GetAssemblies(_assemblies, typeof(TestsInheritanceTests).Assembly)
                        from test in TestsInheritanceFastTests.GetAssemblyTypes(assembly)
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
            var types = from assembly in TestsInheritanceFastTests.GetAssemblies(_assemblies, typeof(TestsInheritanceTests).Assembly)
                        from handler in TestsInheritanceFastTests.GetAssemblyTypes(assembly)
                        where handler != typeof(DatabaseRequestHandler) && handler != typeof(ServerRequestHandler)
                        where handler.IsSubclassOf(typeof(RequestHandler)) && handler.IsSubclassOf(typeof(ServerRequestHandler)) == false && handler.IsSubclassOf(typeof(DatabaseRequestHandler)) == false
                        select handler;

            var array = types.ToArray();
            if (array.Length == 0)
                return;

            var userMessage = string.Join(Environment.NewLine, array.Select(x => x.FullName));
            throw new Exception(userMessage);
        }
    }
}

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
using Xunit;

namespace SlowTests.Tests
{
    public class NoNonDisposableTests : NoDisposalNeeded
    {
        private readonly HashSet<Assembly> _assemblies = new HashSet<Assembly>();

        [Fact]
        public void ShouldExist()
        {
            var types = from assembly in GetAssemblies(typeof(NoNonDisposableTests).GetTypeInfo().Assembly)
                        from test in assembly.GetTypes()
                        where test.GetMethods().Any(x => x.GetCustomAttributes(typeof(FactAttribute), true).Count() != 0 || x.GetCustomAttributes(typeof(TheoryAttribute), true).Count() != 0)
                        where typeof(IDisposable).IsAssignableFrom(test) == false
                        select test;

            var array = types.ToArray();
            if (array.Length == 0)
                return;

            var userMessage = string.Join(Environment.NewLine, array.Select(x => x.FullName));
            throw new Exception(userMessage);
        }

        private IEnumerable<Assembly> GetAssemblies(Assembly assemblyToScan)
        {
            if (_assemblies.Add(assemblyToScan) == false)
                yield break;

            yield return assemblyToScan;

            foreach (var referencedAssembly in assemblyToScan.GetReferencedAssemblies().Select(Assembly.Load))
            {
                foreach (var assembly in GetAssemblies(referencedAssembly))
                    yield return assembly;
            }
        }
    }
}

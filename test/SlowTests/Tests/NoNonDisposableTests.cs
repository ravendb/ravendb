// -----------------------------------------------------------------------
//  <copyright file="NoNonDisposableTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Reflection;
using FastTests;
using Xunit;

namespace SlowTests.Tests
{
    public class NoNonDisposableTests : NoDisposalNeeded
    {
        [Fact]
        public void ShouldExist()
        {
            var types = from test in typeof(NoNonDisposableTests).GetTypeInfo().Assembly.GetTypes()
                        where test.GetMethods().Any(x => x.GetCustomAttributes(typeof(FactAttribute), true).Count() != 0 || x.GetCustomAttributes(typeof(TheoryAttribute), true).Count() != 0)
                        where typeof(IDisposable).IsAssignableFrom(test) == false
                        select test;

            var array = types.ToArray();
            if (array.Length == 0)
                return;

            Assert.True(false, string.Join(Environment.NewLine, array.Select(x => x.FullName)));
        }
    }
}

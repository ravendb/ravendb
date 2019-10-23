// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2984.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Reflection;
using FastTests;
using Raven.Client.Util;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2984 : NoDisposalNeeded
    {
        public RavenDB_2984(ITestOutputHelper output) : base(output)
        {
        }

        private class TestClass
        {
            public string PublicProperty { get; set; }

            public string PublicField;

            private string privateField;

            private string PrivateProperty { get; set; }

            private string internalField;
            public string PublicPropertyWithInternalField
            {
                get
                {
                    return internalField;
                }
            }

            public TestClass()
            {
                PublicProperty = string.Empty;
                PublicField = string.Empty;
                PrivateProperty = string.Empty;

                privateField = string.Empty;
                internalField = string.Empty;
            }
        }

        [Fact]
        public void GetPropertiesAndFieldsForShouldOmittBackingFields()
        {
            var properties = ReflectionUtil
                .GetPropertiesAndFieldsFor<TestClass>(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .ToList();

            Assert.Equal(6, properties.Count);
            Assert.True(properties.Any(x => x.Name == "PublicProperty"));
            Assert.True(properties.Any(x => x.Name == "PublicField"));
            Assert.True(properties.Any(x => x.Name == "privateField"));
            Assert.True(properties.Any(x => x.Name == "PrivateProperty"));
            Assert.True(properties.Any(x => x.Name == "internalField"));
            Assert.True(properties.Any(x => x.Name == "PublicPropertyWithInternalField"));
        }
    }
}

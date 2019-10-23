using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_10520 : NoDisposalNeeded
    {
        public RavenDB_10520(ITestOutputHelper output)
            : base(output)
        {
        }

        [Fact]
        public void DocumentQueriesShouldNotInheritForEnumerableInterface()
        {
            var baseDocumentQuery = typeof(IDocumentQueryBase<>);

            foreach (var type in GetAllTypesThatInheritFrom(baseDocumentQuery))
            {
                var result = type.GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IEnumerable<>));

                Assert.False(result, $"Type '{type.FullName}' should not inherit from IEnumerable<>");

                result = type.GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IEnumerable));

                Assert.False(result, $"Type '{type.FullName}' should not inherit from IEnumerable");
            }
        }

        private static IEnumerable<Type> GetAllTypesThatInheritFrom(Type baseType)
        {
            foreach (var type in baseType.Assembly.GetTypes())
            {
                if (type.IsInterface == false)
                    continue;

                if (type.GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == baseType))
                    yield return type;
            }
        }
    }
}

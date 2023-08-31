using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Raven.Client.Http;
using Raven.Server;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_6341 : RavenTestBase
    {
        public RavenDB_6341(ITestOutputHelper output) : base(output)
        {
        }

        private readonly HashSet<string> _fieldsToIgnore = new HashSet<string>
        {
            nameof(RavenCommand<object>.Result),
            nameof(RavenCommand<object>.FailedNodes),
            nameof(RavenCommand<object>.StatusCode),
            nameof(RavenCommand<object>.CancellationToken),
            nameof(IRaftCommand.RaftUniqueRequestId)
        };

        [Fact]
        public void CommandsShouldNotHavePublicSettersInPropertiesOrNonReadOnlyPublicFields()
        {
            var sb = new StringBuilder();

            foreach (var commandType in GetCommands(new[] { typeof(RavenCommand<>).Assembly, typeof(RavenServer).Assembly }))
            {
                foreach (var property in commandType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    var setMethod = property.GetSetMethod();
                    if (setMethod == null)
                        continue;

                    if (setMethod.IsPublic == false)
                        continue;

                    sb.AppendLine($"Detected public setter in public property: {commandType.Name}.{property.Name}");
                }

                foreach (var field in System.Reflection.TypeExtensions.GetFields(commandType, BindingFlags.Instance | BindingFlags.Public))
                {
                    if (field.IsInitOnly)
                        continue;

                    if (_fieldsToIgnore.Contains(field.Name))
                        continue;

                    sb.AppendLine($"Detected non-readonly public field: {commandType.Name}.{field.Name}");
                }
            }

            if (sb.Length == 0)
                return;

            throw new InvalidOperationException(sb.ToString());
        }

        private static IEnumerable<Type> GetCommands(IEnumerable<Assembly> assemblies)
        {
            foreach (var assembly in assemblies)
            {
                foreach (var commandType in assembly
                    .GetTypes()
                    .Where(x => IsSubclassOfGenericType(typeof(RavenCommand<>), x))
                    .Where(x => x.IsNotPublic == false))
                {
                    yield return commandType;
                }
            }
        }

        private static bool IsSubclassOfGenericType(Type genericType, Type type)
        {
            while (type != null && type != typeof(object))
            {
                var current = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
                if (genericType == current)
                    return true;

                type = type.BaseType;
            }

            return false;
        }
    }
}

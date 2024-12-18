using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_7953 : NoDisposalNeeded
    {
        public RavenDB_7953(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly HashSet<string> StringMethodsToIgnore = new HashSet<string>
        {
            nameof(string.IsNormalized),
            nameof(string.CopyTo),
            nameof(string.Normalize),
            nameof(string.ToString),
            nameof(string.Clone),
            nameof(string.GetTypeCode),
            nameof(string.GetEnumerator),
            nameof(string.Equals),
            nameof(string.GetHashCode),
            nameof(string.GetPinnableReference),
            nameof(string.EnumerateRunes),
            "get_Chars",
            nameof(string.TryCopyTo),
            nameof(string.ReplaceLineEndings)
        };

        [Fact]
        public void StringMethodsShouldBeAvailableInLazyStringValue()
        {
            var stringType = typeof(string);
            var lsvType = typeof(LazyStringValue);

            var sb = new StringBuilder();
            foreach (var method in stringType.GetMethods(BindingFlags.Instance | BindingFlags.Public))
            {
                if (StringMethodsToIgnore.Contains(method.Name))
                    continue;

                var parameters = method
                    .GetParameters();

                var lsvMethod = lsvType.GetMethod(method.Name, parameters.Select(x => x.ParameterType).ToArray());

                if (lsvMethod != null)
                    continue;

                sb.AppendLine($"{method.ReturnType.Name} {method.Name}({string.Join(", ", parameters.Select(x => x.IsOptional ? $"{x.ParameterType.Name} {x.Name} = {x.DefaultValue?.GetType().Name}.{x.DefaultValue}" : $"{x.ParameterType.Name} {x.Name}"))})");
            }

            if (sb.Length == 0)
                return;

            throw new InvalidOperationException("There are some missing methods in LazyStringValue." + Environment.NewLine + sb);
        }
    }
}

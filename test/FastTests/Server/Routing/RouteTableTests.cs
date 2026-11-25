using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Routing
{
    public class RouteTableTests : NoDisposalNeeded
    {
        public RouteTableTests(ITestOutputHelper output) : base(output)
        {
        }

        // The route table is generated at compile time, these tests discover the routes the way the server did at runtime
        // before (reflection over all its types) and verify that the generated table has exactly the same routes.

        private static IEnumerable<MethodInfo> ActionMethods() =>
            typeof(RouteScanner).Assembly.GetTypes()
                .SelectMany(type => type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly));

        private static string NameOf(MethodInfo method) => method.DeclaringType!.FullName!.Replace('+', '.') + "." + method.Name;

        [RavenFact(RavenTestCategory.Core)]
        public void GeneratedActionsMatchTheActionsFoundByReflection()
        {
            var expected = ActionMethods()
                .SelectMany(method => method.GetCustomAttributes<RavenActionAttribute>().Select(attribute =>
                    Describe(NameOf(method), typeof(DatabaseRequestHandler).IsAssignableFrom(method.DeclaringType), attribute)))
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToList();

            var actual = RouteTable.Actions
                .Select(route => Describe(route.Name, route.IsDatabaseHandler, route.Attribute))
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToList();

            Assert.NotEmpty(expected);
            Assert.Equal(expected, actual);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void GeneratedShardedActionsMatchTheShardedActionsFoundByReflection()
        {
            var expected = ActionMethods()
                .SelectMany(method => method.GetCustomAttributes<RavenShardedActionAttribute>().Select(attribute => $"{NameOf(method)} {attribute.Method} {attribute.Path}"))
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToList();

            var actual = RouteTable.ShardedActions
                .Select(route => $"{route.Name} {route.Attribute.Method} {route.Attribute.Path}")
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToList();

            Assert.NotEmpty(expected);
            Assert.Equal(expected, actual);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void GeneratedHandlersCreateTheRightHandlerAndCallTheRightMethod()
        {
            var methodsByName = ActionMethods()
                .Where(method => method.IsDefined(typeof(RavenActionAttribute)) || method.IsDefined(typeof(RavenShardedActionAttribute)))
                .ToDictionary(NameOf);

            var routes = RouteTable.Actions.Select(x => (x.Name, x.Handler))
                .Concat(RouteTable.ShardedActions.Select(x => (x.Name, x.Handler)));

            foreach (var (name, handler) in routes)
            {
                var expected = methodsByName[name];
                var called = CalledMethods(handler.Method);

                Assert.True(called.Contains(expected.DeclaringType!.GetConstructor(Type.EmptyTypes)), $"{name} doesn't create a {expected.DeclaringType}");
                Assert.True(called.Contains(expected), $"{name} doesn't call {expected}");
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void RouteScannerRegistersTheRoutesOfAllTheActions()
        {
            var expected = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var attribute in ActionMethods().SelectMany(method => method.GetCustomAttributes<RavenActionAttribute>()))
            {
                expected.Add(attribute.Method + attribute.Path);
                if (attribute.CorsMode != CorsMode.None)
                    expected.Add("OPTIONS" + attribute.Path);
            }

            var routes = RouteScanner.Scan();

            Assert.True(expected.SetEquals(routes.Keys), "The registered routes don't match the actions");
            Assert.All(routes.Values, route => Assert.NotNull(route.GetRequestHandler()));

            foreach (var attribute in ActionMethods().SelectMany(method => method.GetCustomAttributes<RavenShardedActionAttribute>()))
                Assert.NotNull(routes[attribute.Method + attribute.Path].GetShardedRequestHandler());
        }

        private static string Describe(string name, bool isDatabaseHandler, RavenActionAttribute attribute) =>
            $"{name} {attribute.Method} {attribute.Path} database:{isDatabaseHandler} auth:{attribute.RequiredAuthorization} endpoint:{attribute.EndpointType} " +
            $"skipUsages:{attribute.SkipUsagesCount} skipLastRequestTime:{attribute.SkipLastRequestTimeUpdate} cors:{attribute.CorsMode} " +
            $"debug:{attribute.IsDebugInformationEndpoint} posix:{attribute.IsPosixSpecificEndpoint} cpuCredits:{attribute.DisableOnCpuCreditsExhaustion} " +
            $"checkForChanges:{attribute.CheckForChanges}";

        private static HashSet<MethodBase> CalledMethods(MethodInfo method)
        {
            // the generated handler is tiny: newobj, Init, the action, so looking for call / callvirt / newobj tokens is enough
            const byte Call = 0x28, CallVirt = 0x6F, NewObj = 0x73;

            var il = method.GetMethodBody()!.GetILAsByteArray()!;
            var called = new HashSet<MethodBase>();
            for (var i = 0; i + 4 < il.Length; i++)
            {
                if (il[i] is not (Call or CallVirt or NewObj))
                    continue;

                try
                {
                    called.Add(method.Module.ResolveMethod(BitConverter.ToInt32(il, i + 1))!);
                    i += 4;
                }
                catch (ArgumentException)
                {
                    // an operand byte that happened to look like an opcode
                }
            }

            return called;
        }
    }
}

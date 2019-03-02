using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6596 : NoDisposalNeeded
    {
        private static readonly List<MethodInfo> _syncTests = new List<MethodInfo>();

        private static readonly List<MethodInfo> _asyncTests = new List<MethodInfo>();

        public static IEnumerable<object[]> Cultures()
        {
            yield return new object[] { "he-IL" };
            yield return new object[] { "pl-PL" };
            yield return new object[] { "zh-CN" };
            yield return new object[] { "de-DE" };
            yield return new object[] { "ja-JP" };
            yield return new object[] { "ru-RU" };
            yield return new object[] { "es-ES" };
            yield return new object[] { "th-TH" };
        }

        static RavenDB_6596()
        {
            FindTests(typeof(CRUD).GetTypeInfo().Assembly, typeof(RavenDB_6596).GetTypeInfo().Assembly);
        }

        private static void FindTests(params Assembly[] assemblies)
        {
            foreach (var assembly in assemblies)
            {
                foreach (var type in assembly.GetTypes())
                {
                    foreach (var method in type.GetMethods())
                    {
                        var factAttribute = method.GetCustomAttribute<FactAttribute>();
                        if (factAttribute == null)
                            continue;

                        if (factAttribute is TheoryAttribute)
                            continue;

                        if (PlatformDetails.RunningOnPosix && factAttribute is NonLinuxFactAttribute)
                            continue;

                        if (string.IsNullOrWhiteSpace(factAttribute.Skip) == false)
                            continue;

                        if (method.DeclaringType == typeof(RavenDB_6596))
                            continue;

                        if (method.ReturnType == typeof(void))
                        {
                            _syncTests.Add(method);
                            continue;
                        }

                        if (method.ReturnType == typeof(Task))
                        {
                            _asyncTests.Add(method);
                            continue;
                        }
                    }
                }
            }
        }

        [CultureTheory]
        [MemberData(nameof(Cultures))]
        public async Task Run(string c)
        {
            var culture = new CultureInfo(c);
            var originalCulture = CultureInfo.CurrentCulture;
            var originalUICulture = CultureInfo.CurrentUICulture;
            var originalDefaultCulture = CultureInfo.DefaultThreadCurrentCulture;
            var originalDefaultUICulture = CultureInfo.DefaultThreadCurrentUICulture;

            var builder = new StringBuilder();
            foreach (var test in _syncTests)
            {
                try
                {
                    CultureInfo.CurrentCulture = culture;
                    CultureInfo.CurrentUICulture = culture;
                    CultureInfo.DefaultThreadCurrentCulture = culture;
                    CultureInfo.DefaultThreadCurrentUICulture = culture;

                    Console.WriteLine($"\t\t{test.Name}");

                    try
                    {
                        var @class = Activator.CreateInstance(test.DeclaringType);
                        using (@class as IDisposable)
                        {
                            test.Invoke(@class, null);
                        }
                    }
                    catch (Exception e)
                    {
                        var tie = e as TargetInvocationException;
                        if (tie != null)
                            e = tie.InnerException;

                        builder.AppendLine($"Culture: {culture.Name}. Test: {test.Name}. Message: {e.Message}. StackTrace: {e.StackTrace}");
                    }
                }
                finally
                {
                    CultureInfo.CurrentCulture = originalCulture;
                    CultureInfo.CurrentUICulture = originalUICulture;
                    CultureInfo.DefaultThreadCurrentCulture = originalDefaultCulture;
                    CultureInfo.DefaultThreadCurrentUICulture = originalDefaultUICulture;
                }
            }

            foreach (var test in _asyncTests)
            {
                try
                {
                    CultureInfo.CurrentCulture = culture;
                    CultureInfo.CurrentUICulture = culture;
                    CultureInfo.DefaultThreadCurrentCulture = culture;
                    CultureInfo.DefaultThreadCurrentUICulture = culture;

                    Console.WriteLine($"\t\t{test.Name}");

                    try
                    {
                        var @class = Activator.CreateInstance(test.DeclaringType);
                        using (@class as IDisposable)
                        {
                            await (Task)test.Invoke(@class, null);
                        }
                    }
                    catch (Exception e)
                    {
                        builder.AppendLine($"Culture: {culture.Name}. Test: {test.Name}. Message: {e.Message}");
                    }
                }
                finally
                {
                    CultureInfo.CurrentCulture = originalCulture;
                    CultureInfo.CurrentUICulture = originalUICulture;
                    CultureInfo.DefaultThreadCurrentCulture = originalDefaultCulture;
                    CultureInfo.DefaultThreadCurrentUICulture = originalDefaultUICulture;
                }
            }

            if (builder.Length != 0)
                throw new InvalidOperationException(builder.ToString());
        }
    }
}

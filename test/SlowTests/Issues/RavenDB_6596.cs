using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6596 : NoDisposalNeeded
    {
        private static readonly List<MethodInfo> _syncTests = new List<MethodInfo>();

        private static readonly List<MethodInfo> _asyncTests = new List<MethodInfo>();

        private static IEnumerable<object[]> Cultures()
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
                        if (method.GetCustomAttribute<FactAttribute>() == null)
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

                    try
                    {
                        var @class = Activator.CreateInstance(test.DeclaringType);
                        using ((IDisposable)@class)
                        {
                            test.Invoke(@class, null);
                        }
                    }
                    catch (Exception e)
                    {
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

                    try
                    {
                        var @class = Activator.CreateInstance(test.DeclaringType);
                        using ((IDisposable)@class)
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
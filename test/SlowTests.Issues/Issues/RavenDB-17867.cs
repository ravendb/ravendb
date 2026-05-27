using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_17867 : RavenTestBase
    {
        public RavenDB_17867(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenPlatform.Windows, RavenArchitecture.AllX64)]
        public void Recursion_In_Additional_Sources_Should_Not_Crash_The_Server()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Companies/WithRecursion",
                    Maps =
                    {
                        @"from company in docs.Companies
                          select new
                          {
                              company.Name,
                              Test = PeopleUtil.CalculatePersonEmail(company.Name)
                          }"
                    },
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["PeopleUtil"] = @"
public static class PeopleUtil
{
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining | System.Runtime.CompilerServices.MethodImplOptions.NoOptimization)]
    public static string CalculatePersonEmail(string name)
    {
        return CalculatePersonEmail(name);
    }
}"
                    }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var mapErrors = WaitForValue(() =>
                    store.Maintenance.Send(new GetIndexStatisticsOperation("Companies/WithRecursion")).MapErrors > 0, true);

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation("Companies/WithRecursion"));

                Assert.True(indexStats.MapErrors > 0, "Expected map errors from recursive additional source");
                Assert.Equal(0, indexStats.MapSuccesses);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Companies/WithRecursion" }));
                Assert.True(errors[0].Errors.Length > 0, "Expected index errors to be recorded");
                Assert.True(errors[0].Errors.Any(e => e.Error.Contains("InsufficientExecutionStackException")),
                    "Expected at least one error to contain InsufficientExecutionStackException");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenPlatform.Windows, RavenArchitecture.AllX64)]
        public void Recursion_In_Expression_Bodied_Additional_Sources_Should_Not_Crash_The_Server()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Companies/WithExpressionBodyRecursion",
                    Maps =
                    {
                        @"from company in docs.Companies
                          select new
                          {
                              company.Name,
                              Depth = TreeUtil.GetDepth(company.Name)
                          }"
                    },
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["TreeUtil"] = @"
public static class TreeUtil
{
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining | System.Runtime.CompilerServices.MethodImplOptions.NoOptimization)]
    public static int GetDepth(string name) => 1 + GetDepth(name);
}"
                    }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var mapErrors = WaitForValue(() =>
                    store.Maintenance.Send(new GetIndexStatisticsOperation("Companies/WithExpressionBodyRecursion")).MapErrors > 0, true);

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation("Companies/WithExpressionBodyRecursion"));

                Assert.True(indexStats.MapErrors > 0, "Expected map errors from recursive expression-bodied additional source");
                Assert.Equal(0, indexStats.MapSuccesses);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Companies/WithExpressionBodyRecursion" }));
                Assert.True(errors[0].Errors.Length > 0, "Expected index errors to be recorded");
                Assert.True(errors[0].Errors.Any(e => e.Error.Contains("InsufficientExecutionStackException")),
                    "Expected at least one error to contain InsufficientExecutionStackException");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenPlatform.Windows, RavenArchitecture.AllX64)]
        public void Recursion_In_Local_Function_Additional_Sources_Should_Not_Crash_The_Server()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Companies/WithLocalFunctionRecursion",
                    Maps =
                    {
                        @"from company in docs.Companies
                          select new
                          {
                              company.Name,
                              Test = Helper.Process(company.Name)
                          }"
                    },
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["Helper"] = @"
public static class Helper
{
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining | System.Runtime.CompilerServices.MethodImplOptions.NoOptimization)]
    public static string Process(string name)
    {
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining | System.Runtime.CompilerServices.MethodImplOptions.NoOptimization)]
        string Inner(string n) { return Inner(n); }
        return Inner(name);
    }
}"
                    }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var mapErrors = WaitForValue(() =>
                    store.Maintenance.Send(new GetIndexStatisticsOperation("Companies/WithLocalFunctionRecursion")).MapErrors > 0, true);

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation("Companies/WithLocalFunctionRecursion"));

                Assert.True(indexStats.MapErrors > 0, "Expected map errors from recursive local function in additional source");
                Assert.Equal(0, indexStats.MapSuccesses);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Companies/WithLocalFunctionRecursion" }));
                Assert.True(errors[0].Errors.Length > 0, "Expected index errors to be recorded");
                Assert.True(errors[0].Errors.Any(e => e.Error.Contains("InsufficientExecutionStackException")),
                    "Expected at least one error to contain InsufficientExecutionStackException");
            }
        }

        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}

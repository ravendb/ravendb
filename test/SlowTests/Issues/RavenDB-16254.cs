using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16254 : RavenTestBase
    {
        public RavenDB_16254(ITestOutputHelper output) : base(output)
        {
        }

        private const string HelperSource = @"
public class Helper
{
    public static string Run(string f)
    {
        return Mut(f.GetHashCode());
    }
    private static string Mut(params float[] args)
    {
        return args[0].ToString();
    }
}
";

        [Fact]
        public async Task CanCompileWithPrivateMethodExposingFloatArray()
        {
            using var store = GetDocumentStore();
            var index = new IndexDefinition
            {
                Name = "Index",
                Maps = {"from u in docs.Users select new { Name = Helper.Run(u.Name) }"},
                AdditionalSources =
                {
                    ["helper.cs"] = HelperSource
                }
            };
            await store.Maintenance.SendAsync(new PutIndexesOperation(index));
        }
    }
}

using System.Collections.Generic;
using FastTests;
using Xunit;
using Xunit.Abstractions;
namespace SlowTests.Issues
{
    public class RavenDB_16956 : RavenTestBase
    {
        public RavenDB_16956(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CheckIfSingleNumberCanBeAParameter()
        {
            var inputData = PrepareTestData();
            var parameters = new Dictionary<string, string>()
            {
                {"1", "Test1"}
            };
            var result = Act(inputData, "from Tests where Value = $1", parameters);
            AssertNoError( new []{inputData[0]},result );
        }

        [Fact]
        public void CheckIfTextParameterStillWorksProperly()
        {
            var inputData = PrepareTestData();
            var parameters = new Dictionary<string, string>()
            {
                {"Properly", "Test1"}
            };
            var result = Act(inputData, "from Tests where Value = $Properly", parameters);
            AssertNoError(new[] { inputData[0] }, result);
        }

        [Fact]
        public void CheckIfNumberCanBeAParameter()
        {
            var inputData = PrepareTestData();
            var parameters = new Dictionary<string, string>()
            {
                { "112345", "Test1" }
            };
            var result = Act(inputData, "from Tests where Value = $112345", parameters);
            AssertNoError(new[] { inputData[0] }, result);
        }

        [Fact]
        public void MultipleNumberParametersWithOptionalLetter()
        {
            var inputData = PrepareTestData();
            var parameters = new Dictionary<string, string>()
            {
                { "112345", "Test1" },
                { "9876abc", "Test2"},
                { "2453426", "Test3"},
            };
            var result = Act(inputData, "from Tests where Value = $112345 or Value = $9876abc or Value = $2453426 ", parameters);
            AssertNoError(new[] { inputData[0], inputData[1], inputData[2]}, result);
        }

        private Test[] PrepareTestData()
        {
            var data = new List<Test>();
            for (int i = 1; i < 4; ++i)
                data.Add(
                    new Test
                    {
                        Id = $"tests/{i}-A", 
                        Value = $"Test{i}"
                    });
            
            return data.ToArray();
        }

        private Test[] Act(Test[] data, string query, Dictionary<string,string> parameters)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    foreach (var item in data)
                        session.Store(item);
                    session.SaveChanges();

                    var output = session.Advanced.RawQuery<Test>(query);
                    foreach (var parameter in parameters)
                        output = output.AddParameter(parameter.Key, parameter.Value);
                    return output.ToArray();
                }
            }
        }

        private void AssertNoError(Test[] expected, Test[] fromDatabase)
        {
           Assert.Equal(expected,fromDatabase);
        }

        private class Test
        {
            public string Id { get; set; }
            public string Value { get; set; }
        }
    }
}

// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3008.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3008 : RavenTestBase
    {
        private class Test
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class TestResult
        {
            public string Id { get; set; }
            public string TestId { get; set; }
            public bool Result { get; set; }
        }

        private class TestResultTransformer : AbstractTransformerCreationTask<TestResult>
        {
            public class Result
            {
                public bool TestResult { get; set; }
                public string TestName { get; set; }
            }

            public TestResultTransformer()
            {
                TransformResults = results => from result in results
                                              let test = LoadDocument<Test>(result.TestId)
                                              select new
                                              {
                                                  TestResult = result.Result,
                                                  TestName = test.Name
                                              };
            }
        }

        [Fact]
        public void CanLoadResultsStartingWith()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Test test = null;
                    for (var i = 0; i < 10; i++) // For some reason we need many of these for the test to fail
                    {
                        test = new Test { Name = "test" };
                        session.Store(test);
                    }
                    var result = new TestResult { Result = true, TestId = test.Id, Id = "sample/1/testResults/1" };
                    session.Store(result);
                    session.SaveChanges();
                }

                new TestResultTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    var results =
                        session.Advanced.LoadStartingWith<TestResultTransformer, TestResultTransformer.Result>(
                            "sample/1/testResults/");
                    Assert.NotEmpty(results);
                }
            }
        }
    }
}

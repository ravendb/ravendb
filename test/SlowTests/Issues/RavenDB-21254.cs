using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21254 : RavenTestBase
{
    public RavenDB_21254(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void Can_query_with_nested_or_inside_and(Options options)
    {
        using var store = GetDocumentStore(options);

        const string Query = @"
FROM Items WHERE (
  (
    Last_Payment_Provider = 1
    OR
    IsManuallyStopped = 2
  )
  AND
  Next_Payment_Date >= 3
)
AND
(
  (
    Next_Payment_Date <= 4
    AND
    NumberOfNotifications = 5
  )
  OR
  (
    Next_Payment_Date <= 6
    AND
    NumberOfNotifications = 7
  )
)";
        using (var s = store.OpenSession())
        {
            // shouldn't throw
            s.Advanced.RawQuery<object>(Query).ToList();
        }

    }

}

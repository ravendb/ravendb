using System.Linq;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class JavascriptIntegerDivisionTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Numbers
    {
        public int A { get; set; }

        public int B { get; set; }

        public int C { get; set; }

        public long Big { get; set; }

        public int? Maybe { get; set; }

        public double D { get; set; }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.JavaScript)]
    public void ProjectionsDivideIntegersLikeCSharp()
    {
        var document = new Numbers { A = 7, B = -7, C = 2, Big = 9_000_000_001, Maybe = 9, D = 7 };

        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(document, "numbers/1");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var query = session.Query<Numbers>().Select(x => new
            {
                Positive = x.A / x.C,
                Negative = x.B / x.C,
                Sum = (x.A + x.C) / x.C,
                Nested = x.A / (x.B / x.C),
                Long = x.Big / x.C,
                Nullable = x.Maybe / x.C,
                Floating = x.D / x.C
            });

            Assert.Contains("Math.trunc(", query.ToString());

            var result = query.Single();
            Assert.Equal(document.A / document.C, result.Positive);
            Assert.Equal(document.B / document.C, result.Negative);
            Assert.Equal((document.A + document.C) / document.C, result.Sum);
            Assert.Equal(document.A / (document.B / document.C), result.Nested);
            Assert.Equal(document.Big / document.C, result.Long);
            Assert.Equal(document.Maybe / document.C, result.Nullable);
            Assert.Equal(document.D / document.C, result.Floating);
        }
    }

    [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.JavaScript)]
    public void SubscriptionPredicatesDivideIntegersLikeCSharp()
    {
        using var store = GetDocumentStore();

        string name = store.Subscriptions.Create<Numbers>(x => x.A / x.C == 3);

        Assert.Contains("Math.trunc(", store.Subscriptions.GetSubscriptionState(name).Query);
    }
}

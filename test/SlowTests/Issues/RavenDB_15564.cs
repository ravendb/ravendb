using System.Globalization;
using System.Threading;
using FastTests;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15564 : NoDisposalNeeded
    {
        public RavenDB_15564(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGetDecimalFromLazyStringValueInCultureUsingComma()
        {
            var commaCulture = new CultureInfo("en-us") { NumberFormat = { CurrencyDecimalSeparator = ",", NumberDecimalSeparator = ",", PercentDecimalSeparator = "," } };

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var json = new DynamicJsonValue { ["Value"] = 1.5 };

                using (var blittable = ctx.ReadObject(json, "foo"))
                {
                    var culture = CultureInfo.CurrentCulture;
                    var uICulture = CultureInfo.CurrentUICulture;

                    Thread.CurrentThread.CurrentCulture = commaCulture;
                    Thread.CurrentThread.CurrentUICulture = commaCulture;

                    try
                    {
                        blittable.TryGet<decimal>("Value", out var value);
                        Assert.Equal(1.5M, value, 2);
                    }
                    finally
                    {
                        Thread.CurrentThread.CurrentCulture = culture;
                        Thread.CurrentThread.CurrentUICulture = uICulture;
                    }
                }
            }
        }
    }
}

using Tests.Infrastructure;
using Xunit;
using QueryPlanBuilder = Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder.QueryPlanBuilder;

namespace FastTests.Corax;

public class StructuralKeyBitWidthsTests : RavenTestBase
{
    public StructuralKeyBitWidthsTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void BitFieldsCanHoldAllEnumMembers()
    {
        QueryPlanBuilder.ValidateStructuralKeyBitWidths();
    }
}

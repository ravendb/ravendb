using System.Threading.Tasks;
using NetTopologySuite.Utilities;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

namespace FastTests.Blittable;

public class RavenDB_23980(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Core)]
    public void CanProperlyHandleCreationOfVectorOnSegmentBoundary()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        var conventions = new DocumentConventions();
        var item = new
        {
            Name = new string('a', 4095),
            Vector = new RavenVector<float>([1,2,3])
        };

        using (var json = conventions.Serialization.DefaultConverter.ToBlittable(item, new MetadataAsDictionary(),
                   ctx, conventions.Serialization.CreateSerializer()))
        {
            Assert.NotNull(json.ToString()); // check it does not throws
        }
        using (var json = conventions.Serialization.DefaultConverter.ToBlittable(item, new MetadataAsDictionary(),
                   ctx, conventions.Serialization.CreateSerializer()))
        {
            Assert.NotNull(json.ToString()); // check it does not throws
        }
    }
}

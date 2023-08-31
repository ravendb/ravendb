using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15772_TypeConverter_Fail_To_Convert_Char : ClusterTestBase
    {
        public RavenDB_15772_TypeConverter_Fail_To_Convert_Char(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Convert_Char_To_Blittable()
        {
            var json = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(new ClientConfiguration
            {
                IdentityPartsSeparator = ':'
            });

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var b = context.ReadObject(json, "test");
                var clientConfiguration = JsonDeserializationServer.ClientConfiguration(b); // Shouldnt fail
            }
        }

    }
}


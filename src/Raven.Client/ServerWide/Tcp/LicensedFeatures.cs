using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Tcp
{
    public sealed class LicensedFeatures
    {
        public bool DataCompression;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DataCompression)] = DataCompression
            };
        }
    }
}

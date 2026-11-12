using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.NewtonsoftJson;

namespace Tests.Infrastructure
{
    public static class NewtonsoftTestConventions
    {
        public static DocumentConventions Default => new() { Serialization = new NewtonsoftJsonSerializationConventions() };
    }
}

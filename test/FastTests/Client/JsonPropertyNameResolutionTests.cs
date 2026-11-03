using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class JsonPropertyNameResolutionTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private class User
    {
        [Newtonsoft.Json.JsonProperty("newtonsoft_name")]
        [System.Text.Json.Serialization.JsonPropertyName("stj_name")]
        public string Name { get; set; }
    }

    private class BaseUser
    {
        [Newtonsoft.Json.JsonProperty("base_name")]
        public virtual string Name { get; set; }
    }

    private class DerivedUser : BaseUser
    {
        public override string Name { get; set; }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void NewtonsoftConventionsRenameQueryFieldsByJsonProperty()
    {
        Assert.Contains("where newtonsoft_name = $p0", GetQueryText(new NewtonsoftJsonSerializationConventions()));
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void SystemTextJsonConventionsRenameQueryFieldsByJsonPropertyName()
    {
        Assert.Contains("where stj_name = $p0", GetQueryText(new SystemTextJsonSerializationConventions()));
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void ConventionsWithoutResolverUseAttributeNames()
    {
        var conventions = new DocumentConventions { Serialization = new ConventionsWithoutResolver() };

        Assert.Equal("stj_name", JsonPropertyNameResolver.GetJsonPropertyName(typeof(User).GetProperty(nameof(User.Name)), conventions));
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
    public void SystemTextJsonConventionsRenameOverriddenPropertiesByBaseJsonProperty()
    {
        var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

        Assert.Equal("base_name", JsonPropertyNameResolver.GetJsonPropertyName(typeof(DerivedUser).GetProperty(nameof(DerivedUser.Name)), conventions));

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(new DerivedUser { Name = "a" }, context))
            Assert.Equal(["base_name"], json.GetPropertyNames());
    }

    private string GetQueryText(ISerializationConventions serialization)
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s => s.Conventions.Serialization = serialization
        });

        using var session = store.OpenSession();
        return session.Query<User>().Where(x => x.Name == "a").ToString();
    }

    private class ConventionsWithoutResolver : ISerializationConventions
    {
        private readonly ISerializationConventions _inner = new SystemTextJsonSerializationConventions();

        public DocumentConventions Conventions => _inner.Conventions;

        public void Initialize(DocumentConventions conventions) => _inner.Initialize(conventions);

        public IJsonSerializer CreateSerializer(CreateSerializerOptions options = null) => _inner.CreateSerializer(options);

        public IJsonSerializer CreateDeserializer(CreateDeserializerOptions options = null) => _inner.CreateDeserializer(options);

        public IBlittableJsonConverter DefaultConverter => _inner.DefaultConverter;

        public ISubscriptionsBlittableJsonConverter CreateConverter<T>(SubscriptionBatch<T> batch) => _inner.CreateConverter(batch);

        public ISessionBlittableJsonConverter CreateConverter(InMemoryDocumentSessionOperations session) => _inner.CreateConverter(session);

        public IJsonWriter CreateWriter(JsonOperationContext context) => _inner.CreateWriter(context);

        public object DeserializeEntityFromBlittable(Type type, BlittableJsonReaderObject json) => _inner.DeserializeEntityFromBlittable(type, json);

        public T DeserializeEntityFromBlittable<T>(BlittableJsonReaderObject json) => _inner.DeserializeEntityFromBlittable<T>(json);
    }
}

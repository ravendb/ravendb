using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Indexing.Issues;

public class RavenDB_24858 : RavenTestBase
{
    public RavenDB_24858(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    public async Task FindPropertyNameForIndexDefinitionAsWorkaroundInsteadOfPropertyNameConverter_ShouldWork()
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = documentStore =>
            {
                documentStore.Conventions.FindPropertyNameForIndex = ConvertStripePropertyNamesForIndex;
                documentStore.Conventions.FindPropertyNameForDynamicIndex = ConvertStripePropertyNamesForIndex;
#pragma warning disable CS0618 // Type or member is obsolete
                documentStore.Conventions.FindPropertyNameForIndexDefinition = info => info.Name;
#pragma warning restore CS0618 // Type or member is obsolete
                documentStore.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                {
                    JsonContractResolver = new IgnoreJsonPropertyNamesForSomeNamespaceContractResolver()
                };
            }
        });

        using var session = store.OpenSession();
        var someInstance = new SomeClass
        {
            SomePropertyWithJsonPropertyAttribute = "value 1",
            SomePropertyWithoutJsonPropertyAttribute = "value 2"
        };
        session.Store(someInstance);
        session.SaveChanges();

        await new Index_SomeClass_ByExact_SomeProperty().ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromSeconds(30));

        var result = session.Query<SomeClass, Index_SomeClass_ByExact_SomeProperty>()
            .Where(someClass =>
                someClass.SomePropertyWithJsonPropertyAttribute == someInstance.SomePropertyWithJsonPropertyAttribute &&
                someClass.SomePropertyWithoutJsonPropertyAttribute == someInstance.SomePropertyWithoutJsonPropertyAttribute,
                exact: true)
            .ToArray();

        Assert.Single(result);
        Assert.Equal(someInstance.SomePropertyWithJsonPropertyAttribute, result[0].SomePropertyWithJsonPropertyAttribute);
        Assert.Equal(someInstance.SomePropertyWithoutJsonPropertyAttribute, result[0].SomePropertyWithoutJsonPropertyAttribute);
    }

    private static string ConvertStripePropertyNamesForIndex(Type indexedType, string _, string __, string jsonPropertyAttributeValue)
    {
        var propertyInfo = GetPropertyInfoByJsonPropertyValue(indexedType, jsonPropertyAttributeValue);
        if (propertyInfo != null && IsSomeType(indexedType))
            return propertyInfo.Name;

        return jsonPropertyAttributeValue;
    }

    private static bool IsSomeType(Type type)
    {
        if (type == null)
            return false;

        return type.Namespace?.StartsWith("SlowTests.Client.Indexing.Issues", StringComparison.OrdinalIgnoreCase) == true;
    }

    private static PropertyInfo GetPropertyInfoByJsonPropertyValue(Type type, string value)
    {
        var propertyInfo = type?.GetProperties().SingleOrDefault(info =>
            info.CustomAttributes.Any(customAttributeData =>
                customAttributeData.AttributeType == typeof(JsonPropertyAttribute) &&
                customAttributeData.ConstructorArguments.Single().Value as string == value));

        return propertyInfo == null
            ? null
            : type?.GetProperty(propertyInfo.Name, bindingAttr: BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
    }

    public class IgnoreJsonPropertyNamesForSomeNamespaceContractResolver : DefaultContractResolver
    {
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var property = base.CreateProperty(member, memberSerialization);
            if (IsSomeType(member.DeclaringType))
                property.PropertyName = member.Name;

            return property;
        }
    }

    public class SomeClass
    {
        [JsonProperty("should_be_ignored")]
        public string SomePropertyWithJsonPropertyAttribute { get; set; }

        public string SomePropertyWithoutJsonPropertyAttribute { get; set; }
    }

    public class Index_SomeClass_ByExact_SomeProperty : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<SomeClass>
    {
        public Index_SomeClass_ByExact_SomeProperty()
        {
            Map = items => from item in items
                select new
                {
                    SomePropertyWithJsonPropertyAttribute = item.SomePropertyWithJsonPropertyAttribute,
                    SomePropertyWithoutJsonPropertyAttribute = item.SomePropertyWithoutJsonPropertyAttribute
                };

            Index(nameof(SomeClass.SomePropertyWithJsonPropertyAttribute), Raven.Client.Documents.Indexes.FieldIndexing.Exact);
            Index(nameof(SomeClass.SomePropertyWithoutJsonPropertyAttribute), Raven.Client.Documents.Indexes.FieldIndexing.Exact);
        }
    }
}

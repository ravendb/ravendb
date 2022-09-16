using FastTests;
using Newtonsoft.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;

namespace SlowTests.MailingList;

public class BigIntegerUsage : RavenTestBase
{
    public BigIntegerUsage(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task CanQueryOnBigIntegerValues()
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s =>
            {
                s.Conventions.Serialization = new CustomSerializationConventions();
                s.Conventions.RegisterCustomQueryTranslator<BigInteger>((i) => i.CompareTo(BigInteger.Zero), ConvertMethod);
                
                s.Conventions.RegisterQueryValueConverter((string name, BigInteger value, bool range, out string objValue) =>
                {
                    objValue = value.ToString("D40");
                    return true;
                });

                LinqPathProvider.Result ConvertMethod(LinqPathProvider provider, Expression expression)
                {
                    if (expression is not MethodCallExpression mce)
                        throw new NotSupportedException(expression.ToString());

                    var target = provider.GetPath(mce.Object);
                    object valueFromExpression = provider.GetValueFromExpression(mce.Arguments[1], typeof(BigInteger));
                    if(valueFromExpression is not BigInteger bi)
                        throw new NotSupportedException(expression.ToString() + " should have a BigInteger value");
                    return new LinqPathProvider.Result()
                    {
                        MemberType = typeof(string), 
                        IsNestedPath = false,
                        Path = target.Path,
                        Args = new[]{bi.ToString("D40")}
                    };
                }
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TypeWithBigInt { Value = new BigInteger(ulong.MaxValue) + 1 });

            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            BigInteger threshold = 5;

            IQueryable<TypeWithBigInt> typeWithBigInts = Queryable.Where(session.Query<TypeWithBigInt>(), x => x.Value > threshold);
            var results = await typeWithBigInts
                .ToArrayAsync();


            Assert.Equal(1, results.Length);
        }
    }


    class TypeWithBigInt
    {
        public string Id { get; set; }

        public BigInteger Value { get; set; }
    }

    class CustomSerializationConventions : NewtonsoftJsonSerializationConventions
    {
        public CustomSerializationConventions()
        {
            CustomizeJsonSerializer = CustomizeSerializer;
            CustomizeJsonDeserializer = CustomizeSerializer;
        }

        private static void CustomizeSerializer(JsonSerializer serializer)
        {
            serializer.Converters.Add(new BigIntegerJsonConverter());
        }
    }

    class BigIntegerJsonConverter : JsonConverter<BigInteger>
    {
        public override void WriteJson(JsonWriter writer, BigInteger value, JsonSerializer serializer)
        {
            writer.WriteValue(value.ToString("D40"));
        }

        public override BigInteger ReadJson(JsonReader reader, Type objectType, BigInteger existingValue, bool hasExistingValue,
            JsonSerializer serializer)
        {
            return BigInteger.Parse((string)reader.Value!);
        }
    }
}

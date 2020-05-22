using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_15023 : NoDisposalNeeded
    {
        public RavenDB_15023(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSwitchDefaultComparerInTheDictionary()
        {
            var deserialize = JsonDeserializationBase.GenerateJsonDeserializationRoutine<MyClass>();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(MyClass.RegularDictionary)] = new DynamicJsonValue
                    {
                        ["Key1"] = "Value1",
                        ["Key2"] = "Value2",
                    },
                    [nameof(MyClass.InterfaceDictionary)] = new DynamicJsonValue
                    {
                        ["Key1"] = "Value1",
                        ["Key2"] = "Value2",
                    },
                    [nameof(MyClass.RegularDictionaryWithAttribute)] = new DynamicJsonValue
                    {
                        ["Key1"] = "Value1",
                        ["Key2"] = "Value2",
                    },
                    [nameof(MyClass.InterfaceDictionaryWithAttribute)] = new DynamicJsonValue
                    {
                        ["Key1"] = "Value1",
                        ["Key2"] = "Value2",
                    },
                };

                var json = context.ReadObject(djv, "item");

                var myClass = deserialize(json);

                Assert.Equal(StringComparer.Ordinal, GetComparer(myClass.RegularDictionary));
                Assert.Equal(StringComparer.Ordinal, GetComparer(myClass.InterfaceDictionary));
                Assert.Equal(StringComparer.InvariantCultureIgnoreCase, GetComparer(myClass.RegularDictionaryWithAttribute));
                Assert.Equal(StringComparer.OrdinalIgnoreCase, GetComparer(myClass.InterfaceDictionaryWithAttribute));
            }
        }

        private static IEqualityComparer<TType> GetComparer<TType>(IDictionary<TType, object> dictionary)
        {
            return ((Dictionary<TType, object>)dictionary).Comparer;
        }

        private class MyClass
        {
            public Dictionary<string, object> RegularDictionary { get; set; }

            public IDictionary<string, object> InterfaceDictionary { get; set; }

            [JsonDeserializationDictionary(StringComparison.InvariantCultureIgnoreCase)]
            public Dictionary<string, object> RegularDictionaryWithAttribute { get; set; }

            [JsonDeserializationDictionary(StringComparison.OrdinalIgnoreCase)]
            public IDictionary<string, object> InterfaceDictionaryWithAttribute { get; set; }
        }
    }
}

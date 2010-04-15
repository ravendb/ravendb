using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Xunit;

namespace Raven.Tests.Patching
{
    public class NestedPatching
    {
        private readonly JObject doc = JObject.Parse(@"{ title: ""A Blog Post"", body: ""html markup"", comments: [{""author"":""ayende"",""text"":""good post 1""},{author: ""ayende"", text:""good post 2""}], ""user"": { ""name"": ""ayende"", ""id"": 13} }");

        [Fact]
        public void SetValueInNestedElement()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(
                    @"[{ ""type"": ""modify"" , ""name"": ""user"", ""value"": [{ ""type"":""set"",""name"":""name"",""value"":""rahien""} ]}]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""user"":{""name"":""rahien"",""id"":13}}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void SetValueInNestedElement_WithConcurrency_Ok()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(
                    @"[{ ""type"": ""modify"" , ""name"": ""user"", ""value"": [{ ""type"":""set"",""name"":""name"",""value"":""rahien""} ], ""prevVal"": { ""name"": ""ayende"", ""id"": 13}}]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""user"":{""name"":""rahien"",""id"":13}}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void SetValueInNestedElement_WithConcurrency_Error()
        {
            Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
                JArray.Parse(
                    @"[{ ""type"": ""modify"" , ""name"": ""user"", ""value"": [{ ""type"":""set"",""name"":""name"",""value"":""rahien""} ], ""prevVal"": { ""name"": ""ayende"", ""id"": 14}}]")
                                                            ));
        }


        [Fact]
        public void RemoveValueInNestedElement()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(
                    @"[{ ""type"": ""modify"" , ""name"": ""user"", ""value"": [{ ""type"":""unset"",""name"":""name""} ]}]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""user"":{""id"":13}}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void SetValueNestedInArray()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(
                    @"[{ ""type"": ""modify"",  ""name"": ""comments"", ""position"": 1, ""value"": [{ ""type"":""set"",""name"":""author"",""value"":""oren""} ]}]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""oren"",""text"":""good post 2""}],""user"":{""name"":""ayende"",""id"":13}}",
                patchedDoc.ToString(Formatting.None));
        }
    }
}
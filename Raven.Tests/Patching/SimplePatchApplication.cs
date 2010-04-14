using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;
using Xunit;

namespace Raven.Tests.Patching
{
    public class SimplePatchApplication
    {
        private readonly JObject doc = 
            JObject.Parse(@"{ title: ""A Blog Post"", body: ""html markup"", comments: [ {author: ""ayende"", text:""good post""}] }");

        [Fact]
        public void PropertyAddition()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{  ""type"":""set"",""name"": ""blog_id"", ""value"": 1 }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""blog_id"":1}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertySet()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""set"", ""name"": ""title"", ""value"": ""another"" }]")
                );

            Assert.Equal(@"{""title"":""another"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertySetToNull()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""set"", ""name"": ""title"", ""value"": null }]")
                );

            Assert.Equal(@"{""title"":null,""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertyRemoval()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""unset"" , ""name"": ""body"" }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertyRemovalPropertyDoesNotExists()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"": ""unset"", ""name"": ""ip"" }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }
    }
}
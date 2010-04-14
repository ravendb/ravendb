using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Xunit;

namespace Raven.Tests.Patching
{
    public class ArrayPatching
    {
        private readonly JObject doc = JObject.Parse(@"{ title: ""A Blog Post"", body: ""html markup"", comments: [{""author"":""ayende"",""text"":""good post 1""},{author: ""ayende"", text:""good post 2""}] }");

        [Fact]
        public void AddingItemToArray()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"": ""add"" , ""name"": ""comments"", ""value"": {""author"":""oren"",""text"":""agreed""} }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""},{""author"":""oren"",""text"":""agreed""}]}", 
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void AddingItemToArray_WithConcurrency_Ok()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"": ""add"" , ""name"": ""comments"", ""value"": {""author"":""oren"",""text"":""agreed""}, ""prevVal"": [{""author"":""ayende"",""text"":""good post 1""},{author: ""ayende"", text:""good post 2""}] }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""},{""author"":""oren"",""text"":""agreed""}]}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void AddingItemToArray_WithConcurrency_Error()
        {
            Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
                JArray.Parse(
                    @"[{ ""type"": ""add"" , ""name"": ""comments"", ""value"": {""author"":""oren"",""text"":""agreed""}, ""prevVal"": [{""author"":""ayende"",""text"":""good post 1""}] }]")
                                                            ));
        }


        [Fact]
        public void AddingItemToArrayWhenArrayDoesNotExists()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""add"",  ""name"": ""blog_id"", ""value"": 1 }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""blog_id"":[1]}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void AddingItemToArrayWhenArrayDoesNotExists_WithConcurrency_Ok()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""add"",  ""name"": ""blog_id"", ""value"": 1, ""prevVal"": undefined }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""blog_id"":[1]}",
                patchedDoc.ToString(Formatting.None));
        }


        [Fact]
        public void AddingItemToArrayWhenArrayDoesNotExists_WithConcurrency_Error()
        {
            Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""add"",  ""name"": ""blog_id"", ""value"": 1, ""prevVal"": [] }]")
                                                            ));

        }

        [Fact]
        public void CanAddServeralItemsToSeveralDifferentPartsAtTheSameTime()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"": ""add"", ""name"": ""blog_id"", ""value"": 1}, { ""type"": ""add"", ""name"": ""blog_id"", ""value"": 2 },{ ""type"": ""set"", ""name"": ""title"", ""value"": ""abc"" } ]")
                );

            Assert.Equal(@"{""title"":""abc"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""blog_id"":[1,2]}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void RemoveItemFromArray()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""remove"",  ""name"": ""comments"", ""position"": 0 }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 2""}]}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void RemoveItemFromArray_WithConcurrency_Ok()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""remove"",  ""name"": ""comments"", ""position"": 0, ""prevVal"": [{""author"":""ayende"",""text"":""good post 1""},{author: ""ayende"", text:""good post 2""}] }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 2""}]}",
                patchedDoc.ToString(Formatting.None));
        }


        [Fact]
        public void RemoveItemFromArray_WithConcurrency_Error()
        {
            Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
                JArray.Parse(
                    @"[{ ""type"":""remove"",  ""name"": ""comments"", ""position"": 0, ""prevVal"": [{""author"":""ayende"",""text"":""different value""},{author: ""ayende"", text:""good post 2""}] }]")
                                                            ));
        }

        [Fact]
        public void InsertItemToArray()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""insert"",  ""name"": ""comments"", ""position"": 1, ""value"": {""author"":""ayende"",""text"":""good post 1.5""} }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 1.5""},{""author"":""ayende"",""text"":""good post 2""}]}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void InsertItemToArray_WithConcurrency_Ok()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                JArray.Parse(@"[{ ""type"":""insert"",  ""name"": ""comments"", ""position"": 1, ""value"": {""author"":""ayende"",""text"":""good post 1.5""}, ""prevVal"": [{""author"":""ayende"",""text"":""good post 1""},{author: ""ayende"", text:""good post 2""}] }]")
                );

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 1.5""},{""author"":""ayende"",""text"":""good post 2""}]}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void InsertItemToArray_WithConcurrency_Error()
        {
            Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
                JArray.Parse(
                    @"[{ ""type"":""insert"",  ""name"": ""comments"", ""position"": 1, ""value"": {""author"":""ayende"",""text"":""good post 1.5""}, ""prevVal"": [{""author"":""different author"",""text"":""good post 1""},{author: ""ayende"", text:""good post 2""}] }]")
                                                            ));
        }
    }
}
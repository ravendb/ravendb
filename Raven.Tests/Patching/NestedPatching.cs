using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Raven.Http.Exceptions;
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
        		new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Modify,
        				Name = "user",
        				Nested = new[]
        				{
        					new PatchRequest {Type = PatchCommandType.Set, Name = "name", Value = new JValue("rahien")},
        				}
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""user"":{""name"":""rahien"",""id"":13}}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void SetValueInNestedElement_WithConcurrency_Ok()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Modify,
        				Name = "user",
						PrevVal = JObject.Parse(@"{ ""name"": ""ayende"", ""id"": 13}"),
        				Nested = new[]
        				{
        					new PatchRequest {Type = PatchCommandType.Set, Name = "name", Value = new JValue("rahien")},
        				}
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""user"":{""name"":""rahien"",""id"":13}}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void SetValueInNestedElement_WithConcurrency_Error()
        {
        	Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
        		new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Modify,
        				Name = "user",
        				PrevVal = JObject.Parse(@"{ ""name"": ""ayende"", ""id"": 14}"),
        				Nested = new[]
        				{
        					new PatchRequest {Type = PatchCommandType.Set, Name = "name", Value = new JValue("rahien")},
        				}
        			},
        		}));
        }


        [Fact]
        public void RemoveValueInNestedElement()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Modify,
        				Name = "user",
        				PrevVal = JObject.Parse(@"{ ""name"": ""ayende"", ""id"": 13}"),
        				Nested = new[]
        				{
        					new PatchRequest {Type = PatchCommandType.Unset, Name = "name" },
        				}
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""ayende"",""text"":""good post 2""}],""user"":{""id"":13}}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void SetValueNestedInArray()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Modify,
        				Name = "comments",
						Position = 1,
        				Nested = new[]
        				{
        					new PatchRequest {Type = PatchCommandType.Set, Name = "author", Value = new JValue("oren")},
        				}
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""oren"",""text"":""good post 2""}],""user"":{""name"":""ayende"",""id"":13}}",
                patchedDoc.ToString(Formatting.None));
        }
    }
}

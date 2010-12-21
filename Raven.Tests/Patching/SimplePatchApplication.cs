//-----------------------------------------------------------------------
// <copyright file="SimplePatchApplication.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Raven.Http.Exceptions;
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
        		new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Set,
        				Name = "blog_id",
        				Value = new JValue(1)
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""blog_id"":1}",
                patchedDoc.ToString(Formatting.None));
        }

		[Fact]
		public void PropertyCopy()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Copy,
        				Name = "comments",
        				Value = new JValue("cmts")
        			},
        		});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""cmts"":[{""author"":""ayende"",""text"":""good post""}]}",
				patchedDoc.ToString(Formatting.None));
		}
        
        [Fact]
		public void PropertyCopyNonExistingProperty()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Copy,
        				Name = "non-existing",
        				Value = new JValue("irrelevant")
        			},
        		});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}",
				patchedDoc.ToString(Formatting.None));
		}

		[Fact]
		public void PropertyMove()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Rename,
        				Name = "comments",
        				Value = new JValue("cmts")
        			},
        		});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""cmts"":[{""author"":""ayende"",""text"":""good post""}]}",
				patchedDoc.ToString(Formatting.None));
		}
        
        [Fact]
		public void PropertyRenameNonExistingProperty()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Rename,
        				Name = "doesnotexist",
        				Value = new JValue("irrelevant")
        			},
        		});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}",
				patchedDoc.ToString(Formatting.None));
		}

        [Fact]
        public void PropertyIncrement()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Set,
        				Name = "blog_id",
        				Value = new JValue(1)
        			},
        		});

            patchedDoc = new JsonPatcher(patchedDoc).Apply(
                new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Inc,
        				Name = "blog_id",
        				Value = new JValue(1)
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""blog_id"":2}",
                patchedDoc.ToString(Formatting.None));
        }
        
        [Fact]
        public void PropertyIncrementOnNonExistingProperty()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Inc,
        				Name = "blog_id",
        				Value = new JValue(1)
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""blog_id"":1}",
                patchedDoc.ToString(Formatting.None));
        }
        
        [Fact]
        public void PropertyAddition_WithConcurrenty_MissingProp()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
               new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Set,
        				Name = "blog_id",
        				Value = new JValue(1),
						PrevVal = JObject.Parse("{'a': undefined}").Property("a").Value
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""blog_id"":1}",
                patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertyAddition_WithConcurrenty_NullValueOnMissingPropShouldThrow()
        {
            Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
               new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Set,
        				Name = "blog_id",
        				Value = new JValue(1),
        				PrevVal = new JValue((object)null)
        			},
        		}));
        }

        [Fact]
        public void PropertyAddition_WithConcurrenty_BadValueOnMissingPropShouldThrow()
        {
            Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
				new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Set,
        				Name = "blog_id",
        				Value = new JValue(1),
        				PrevVal =  new JValue(2)
        			},
        		}));
        }

        [Fact]
        public void PropertyAddition_WithConcurrenty_ExistingValueOn_Ok()
        {
            JObject apply = new JsonPatcher(doc).Apply(
                new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Set,
        				Name = "body",
        				Value = new JValue("differnt markup"),
        				PrevVal = new JValue("html markup")
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""differnt markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", apply.ToString(Formatting.None));
        }


        [Fact]
        public void PropertySet()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
				 new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Set,
        				Name = "title",
        				Value = new JValue("another")
        			},
        		});

            Assert.Equal(@"{""title"":""another"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertySetToNull()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
				 new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Set,
        				Name = "title",
        				Value = new JValue((object)null)
        			},
        		});

            Assert.Equal(@"{""title"":null,""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertyRemoval()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                 new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Unset,
        				Name = "body",
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertyRemoval_WithConcurrency_Ok()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
				 new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Unset,
        				Name = "body",
						PrevVal = new JValue("html markup")
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }

        [Fact]
        public void PropertyRemoval_WithConcurrency_OnError()
        {
            Assert.Throws<ConcurrencyException>(() => new JsonPatcher(doc).Apply(
                 new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Unset,
        				Name = "body",
						PrevVal = new JValue("bad markup")
        			},
        		}));
        }

        [Fact]
        public void PropertyRemovalPropertyDoesNotExists()
        {
            var patchedDoc = new JsonPatcher(doc).Apply(
                new[]
        		{
        			new PatchRequest
        			{
        				Type = PatchCommandType.Unset,
        				Name = "ip",
        			},
        		});

            Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}", patchedDoc.ToString(Formatting.None));
        }
    }
}

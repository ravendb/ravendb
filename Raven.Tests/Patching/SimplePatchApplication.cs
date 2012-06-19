//-----------------------------------------------------------------------
// <copyright file="SimplePatchApplication.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Xunit;

namespace Raven.Tests.Patching
{
	public class SimplePatchApplication
	{
		private readonly RavenJObject doc = 
			RavenJObject.Parse(@"{ title: ""A Blog Post"", body: ""html markup"", comments: [ {author: ""ayende"", text:""good post""}] }");

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
						Value = new RavenJValue(1)
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
						Value = new RavenJValue("cmts")
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
						Value = new RavenJValue("irrelevant")
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
						Value = new RavenJValue("cmts")
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
						Value = new RavenJValue("irrelevant")
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
						Value = new RavenJValue(1)
					},
				});

			patchedDoc = new JsonPatcher(patchedDoc).Apply(
				new[]
				{
					new PatchRequest
					{
						Type = PatchCommandType.Inc,
						Name = "blog_id",
						Value = new RavenJValue(1)
					},
				});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""blog_id"":2}",
				patchedDoc.ToString(Formatting.None));
		}


		[Fact]
		public void ExistingPropertySetToObject()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
				{
					new PatchRequest
					{
						Type = PatchCommandType.Set,
						Name = "title",
						Value = new RavenJObject
						{
							{"a", "b"}
						}
					},
				});

			Assert.Equal(@"{""title"":{""a"":""b""},""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}",
				patchedDoc.ToString(Formatting.None));
		}

		[Fact]
		public void ExistingPropertySetToArray()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
				{
					new PatchRequest
					{
						Type = PatchCommandType.Set,
						Name = "title",
						Value = new RavenJArray()
					},
				});

			Assert.Equal(@"{""title"":[],""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}]}",
				patchedDoc.ToString(Formatting.None));
		}

		[Fact]
		public void NewPropertySetToObject()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
				{
					new PatchRequest
					{
						Type = PatchCommandType.Set,
						Name = "blog_id",
						Value = new RavenJObject
						{
							{"a", "b"}
						}
					},
				});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""blog_id"":{""a"":""b""}}",
				patchedDoc.ToString(Formatting.None));
		}

		[Fact]
		public void NewPropertySetToArray()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
				{
					new PatchRequest
					{
						Type = PatchCommandType.Set,
						Name = "blog_id",
						Value = new RavenJArray()
					},
				});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post""}],""blog_id"":[]}",
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
						Value = new RavenJValue(1)
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
						Value = new RavenJValue(1),
						PrevVal = RavenJObject.Parse("{'a': undefined}")["a"]
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
						Value = new RavenJValue(1),
						PrevVal = new RavenJValue((object)null)
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
						Value = new RavenJValue(1),
						PrevVal =  new RavenJValue(2)
					},
				}));
		}

		[Fact]
		public void PropertyAddition_WithConcurrenty_ExistingValueOn_Ok()
		{
			RavenJObject apply = new JsonPatcher(doc).Apply(
				new[]
				{
					new PatchRequest
					{
						Type = PatchCommandType.Set,
						Name = "body",
						Value = new RavenJValue("differnt markup"),
						PrevVal = new RavenJValue("html markup")
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
						Value = new RavenJValue("another")
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
						Value = new RavenJValue((object)null)
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
						PrevVal = new RavenJValue("html markup")
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
						PrevVal = new RavenJValue("bad markup")
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

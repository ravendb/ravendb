//-----------------------------------------------------------------------
// <copyright file="NestedPatching.cs" company="Hibernating Rhinos LTD">
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
	public class NestedPatching
	{
		private readonly RavenJObject doc = RavenJObject.Parse(@"{ title: ""A Blog Post"", body: ""html markup"", comments: [{""author"":""ayende"",""text"":""good post 1""},{author: ""ayende"", text:""good post 2""}], ""user"": { ""name"": ""ayende"", ""id"": 13} }");
		[Fact]
		public void RenameSecondItemInArray()
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
							new PatchRequest {Type = PatchCommandType.Rename, Name = "author", Value = "authorname"},
						}
					},
				});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""text"":""good post 2"",""authorname"":""ayende""}],""user"":{""name"":""ayende"",""id"":13}}",
				patchedDoc.ToString(Formatting.None));
		}
		[Fact]
		public void RenameAllItemsInArray()
		{
			var patchedDoc = new JsonPatcher(doc).Apply(
				new[]
					{
						new PatchRequest
							{
								Type = PatchCommandType.Modify,
								Name = "comments",
								AllPositions = true,
						Nested = new[]
						{
							new PatchRequest {Type = PatchCommandType.Rename, Name = "author", Value = "authorname"},
						}
					},
				});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""text"":""good post 1"",""authorname"":""ayende""},{""text"":""good post 2"",""authorname"":""ayende""}],""user"":{""name"":""ayende"",""id"":13}}",
				patchedDoc.ToString(Formatting.None));
		}
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
							new PatchRequest {Type = PatchCommandType.Set, Name = "name", Value = new RavenJValue("rahien")},
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
						PrevVal = RavenJObject.Parse(@"{ ""name"": ""ayende"", ""id"": 13}"),
						Nested = new[]
						{
							new PatchRequest {Type = PatchCommandType.Set, Name = "name", Value = new RavenJValue("rahien")},
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
						PrevVal = RavenJObject.Parse(@"{ ""name"": ""ayende"", ""id"": 14}"),
						Nested = new[]
						{
							new PatchRequest {Type = PatchCommandType.Set, Name = "name", Value = new RavenJValue("rahien")},
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
						PrevVal = RavenJObject.Parse(@"{ ""name"": ""ayende"", ""id"": 13}"),
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
							new PatchRequest {Type = PatchCommandType.Set, Name = "author", Value = new RavenJValue("oren")},
						}
					},
				});

			Assert.Equal(@"{""title"":""A Blog Post"",""body"":""html markup"",""comments"":[{""author"":""ayende"",""text"":""good post 1""},{""author"":""oren"",""text"":""good post 2""}],""user"":{""name"":""ayende"",""id"":13}}",
				patchedDoc.ToString(Formatting.None));
		}
	}
}

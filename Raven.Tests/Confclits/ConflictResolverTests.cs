using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Confclits
{
	public class ConflictResolverTests
	{
		[Fact]
		public void CanResolveEmpty()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject(), new RavenJObject());
			Assert.Equal("{}", conflictsResolver.Resolve());
		}

		[Fact]
		public void CanResolveIdentical()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject
			{
				{"Name", "Oren"}
			}, new RavenJObject
			{
				{"Name","Oren"}
			});
			Assert.Equal(new RavenJObject
			{
				{"Name","Oren"}
			}.ToString(Formatting.Indented), conflictsResolver.Resolve());
		}

		[Fact]
		public void CanMergeAdditionalProperties()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject
			{
				{"Name", "Oren"}
			}, new RavenJObject
			{
				{"Age",2}
			});
			Assert.Equal(new RavenJObject
			{
				{"Name","Oren"},
				{"Age", 2}
			}.ToString(Formatting.Indented), conflictsResolver.Resolve());
		}

		[Fact]
		public void CanDetectAndSuggestOptionsForConflict_SimpleProp()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject
			{
				{"Name", "Oren"}
			}, new RavenJObject
			{
					{"Name", "Ayende"}
			});
			Assert.Equal(@"{
  ""Name"": /*>>>> conflict start*/ [
    ""Oren"",
    ""Ayende""
  ]/*<<<< conflict end*/
}", conflictsResolver.Resolve());
		}

		[Fact]
		public void CanMergeProperties_Nested()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject
			{
				{"Name", new RavenJObject
				{
					{"First", "Oren"}
				}}
			}, new RavenJObject
			{
					{"Name", new RavenJObject
					{
						{"Last", "Eini"}	
					}}
			});
			Assert.Equal(@"{
  ""Name"":{
    ""First"": ""Oren"",
    ""Last"": ""Eini""
  }
}", conflictsResolver.Resolve());
		}

		[Fact]
		public void CanDetectConflict_DifferentValues()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject
			{
				{"Name", new RavenJObject
				{
					{"First", "Oren"}
				}}
			}, new RavenJObject
			{
					{"Name",  "Eini"}
			});
			Assert.Equal(@"{
  ""Name"": /*>>>> conflict start*/ [
    {
      ""First"": ""Oren""
    },
    ""Eini""
  ]/*<<<< conflict end*/
}", conflictsResolver.Resolve());
		}

		[Fact]
		public void CanDetectAndSuggestOptionsForConflict_NestedProp()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject
			{
				{"Name", "Oren"}
			}, new RavenJObject
			{
					{"Name", "Ayende"}
			});
			Assert.Equal(@"{
  ""Name"": /*>>>> conflict start*/ [
    ""Oren"",
    ""Ayende""
  ]/*<<<< conflict end*/
}", conflictsResolver.Resolve());
		}

		[Fact]
		public void CanMergeArrays()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject
			{
				{"Nicks", new RavenJArray{"Oren"}}
			}, new RavenJObject
			{
				{"Nicks", new RavenJArray{"Ayende"}}
			});
			Assert.Equal(@"{
  ""Nicks"": /*>>>> auto merged array start*/ [
    ""Oren"",
    ""Ayende""
  ]/*<<<< auto merged array end*/
}", conflictsResolver.Resolve());
		}

		[Fact]
		public void CanMergeArrays_SameStart()
		{
			var conflictsResolver = new ConflictsResolver(new RavenJObject
			{
				{"Comments", new RavenJArray{1,2,4}}
			}, new RavenJObject
			{
				{"Comments", new RavenJArray{1,2,5}}
			});
			Assert.Equal(@"{
  ""Comments"": /*>>>> auto merged array start*/ [
    1,
    2,
    4,
    5
  ]/*<<<< auto merged array end*/
}", conflictsResolver.Resolve());
		}
	}
}
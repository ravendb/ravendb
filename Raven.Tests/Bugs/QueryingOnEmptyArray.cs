using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingOnEmptyArray : RavenTest
	{
		[Fact]
		public void CanGetResults()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("test", null,
				                           RavenJObject.Parse(
				                           	@"{
 ""Value"": ""auto"",
 ""Translations"": [
   {
	 ""Value"": ""auto"",
	 ""LanguageCode"": ""en-EN""
   }
 ],
 ""AliasList"": [],
 ""Path"": null,
 ""KType"": ""tag"",
 ""Disabled"": false
}"),
				                           new RavenJObject());

				var queryResult = store.DatabaseCommands.Query("dynamic", new IndexQuery
				{
					Query = @"(Translations,LanguageCode:en\-EN AND Translations,Value:auto*) OR AliasList,:auto*"
				}, new string[0]);

				Assert.NotEmpty(queryResult.Results);
			}
		}
	}
}

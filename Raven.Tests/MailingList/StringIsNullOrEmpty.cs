using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class StringIsNullOrEmpty : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			dynamic doc = new DynamicJsonObject(new RavenJObject());

			Assert.True(string.IsNullOrEmpty(doc.Name));
		} 
	}
}
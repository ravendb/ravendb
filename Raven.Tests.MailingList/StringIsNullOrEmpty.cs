using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class StringIsNullOrEmpty : NoDisposalNeeded
	{
		[Fact]
		public void ShouldWork()
		{
			dynamic doc = new DynamicJsonObject(new RavenJObject());

			Assert.True(string.IsNullOrEmpty(doc.Name));
		} 
	}
}
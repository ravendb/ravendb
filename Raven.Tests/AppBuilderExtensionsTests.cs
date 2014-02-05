using Microsoft.Owin.Builder;
using Owin;
using Raven.Database.Config;
using Xunit;

namespace Raven.Tests
{
	public class AppBuilderExtensionsTests : RavenTest
	{
		[Fact]
		public void When_HostOnAppDisposing_key_not_exist_then_should_not_throw()
		{
			string path = NewDataPath();
			Assert.DoesNotThrow(() => new AppBuilder().UseRavenDB(new InMemoryRavenConfiguration{ DataDirectory = path }));
		}
	}
}
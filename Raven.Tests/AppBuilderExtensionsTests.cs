using Microsoft.Owin.Builder;
using Owin;
using Xunit;

namespace Raven.Tests
{
	public class AppBuilderExtensionsTests
	{
		[Fact]
		public void When_HostOnAppDisposing_key_not_exist_then_should_not_throw()
		{
			Assert.DoesNotThrow(() => new AppBuilder().UseRavenDB());
		}
	}
}
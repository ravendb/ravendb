using System;
using Raven.Abstractions.Connection;
using Raven.Tests.Bugs.Identifiers;
using System.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class IisQueryLengthIssues : IisExpressTestClient
	{
		private readonly string[] errorOptions = new[]
										{
											"configuration/system.webServer/security/requestFiltering/requestLimits@maxQueryString",
											"maxQueryStringLength"
										};

		[IISExpressInstalledFact]
		public void ShouldFailGracefully()
		{
			using (var store = NewDocumentStore())
			{
				var name = new string('x', 0x1000);
				var exception = Assert.Throws<InvalidOperationException>(() => store.OpenSession().Query<User>().Where(u => u.FirstName == name).ToList());
				
				Assert.True(errorOptions.Any(s => exception.Message.Contains(s)));
			}
		}

		[IISExpressInstalledFact]
		public void ShouldFailGracefully_StaticIndex()
		{
			using (var store = NewDocumentStore())
			{
				var name = new string('x', 0x1000);
				var exception = Assert.Throws<InvalidOperationException>(() => store.OpenSession().Query<User>("test").Where(u => u.FirstName == name).ToList());
				Assert.True(errorOptions.Any(s => exception.Message.Contains(s)));
			}
		}
	}
}

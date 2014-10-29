using System;
using System.Linq;

using Raven.Tests.Common.Attributes;
using Raven.Tests.Common.Util;

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
				Assert.Throws<InvalidOperationException>(() => store.OpenSession().Query<User>().Where(u => u.FirstName == name).ToList());
			}
		}

		[IISExpressInstalledFact]
		public void ShouldFailGracefully_StaticIndex()
		{
			using (var store = NewDocumentStore())
			{
				var name = new string('x', 0x1000);
				Assert.Throws<InvalidOperationException>(() => store.OpenSession().Query<User>("test").Where(u => u.FirstName == name).ToList());
			}
		}
	}
}
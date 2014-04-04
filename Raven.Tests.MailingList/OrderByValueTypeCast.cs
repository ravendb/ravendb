using System;
using System.Linq;
using System.Linq.Expressions;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class OrderByValueTypeCast : RavenTest
	{
		[Fact]
		public void Test()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					Expression<Func<User, object>> sort = x => x.LastName.Length;
					s.Query<User>()
						.OrderBy(sort)
						.ToList();

				}
			}
		}
	}
}
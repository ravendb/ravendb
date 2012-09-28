using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class Mare : RavenTest
	{
		 [Fact]
		 public void CanUnderstandEqualsMethod()
		 {
			 using(var store = NewDocumentStore())
			 {
				 using(var s = store.OpenSession())
				 {
					 s.Query<User>().Where(x => x.Age.Equals(10)).ToList();
				 }
			 }
		 }

		 public class User
		 {
			 public int Age { get; set; }
		 }
	}
}
using Xunit;

namespace Raven.Tests.Bugs
{
	public class NullableEnum : RavenTest
	{
		[Fact]
		public void CanSerializeAndDeserializeCorrectly()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new Person{});
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var person = s.Load<Person>("people/1");
					Assert.Null(person.Gender);
				}
			}
		}

		public class Person
		{
			public Gender? Gender { get; set; }
		}

		public enum Gender
		{
			Male,
			Female
		}
	}
}

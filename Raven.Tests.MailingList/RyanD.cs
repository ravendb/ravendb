using System;
using System.Reflection;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class RyanD : RavenTest
	{
		[Fact]
		public void Changing_FindIdentityProperty_breaks_loading()
		{
			using(var store = NewDocumentStore())
			{
				store.Conventions.FindIdentityProperty = FindGuidIdentityProperty;

				var id = Guid.NewGuid();
				var name = "bar";

				using (var session = store.OpenSession())
				{
					var entity = new BaseOne() { BaseOneGuid = id, Name = name };
					session.Store(entity);
					session.SaveChanges();
				}

				using (var session2 = store.OpenSession())
				{
					var entity = session2.Load<BaseOne>(id);
					Assert.NotNull(entity);
					Assert.Equal(name, entity.Name);
				}
			}
		}

		private bool FindGuidIdentityProperty(PropertyInfo propertyInfo)
		{
			var found = propertyInfo.Name == propertyInfo.DeclaringType.Name + "Guid";
			return found;
		}


		public class BaseOne
		{
			public Guid BaseOneGuid { get; set; }
			public string Name { get; set; }
		}

	}
}
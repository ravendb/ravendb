using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class JohanNilsson : RavenTest
	{
		internal interface IEntity
		{
			string Id2 { get; set; }
		}

		internal interface IDomainObject : IEntity
		{
			string ImportantProperty { get; }
		}

		class DomainObject : IDomainObject
		{
			public string Id2 { get; set; }
			public string ImportantProperty { get; set; }
		}

		[Fact]
		public void WithCustomizedTagNameAndIdentityProperty()
		{
			var id = string.Empty;
			using (var store = NewDocumentStore())
			{
				store.Conventions.AllowQueriesOnId = true;
				var defaultFindIdentityProperty = store.Conventions.FindIdentityProperty;
				store.Conventions.FindIdentityProperty = property =>
					typeof(IEntity).IsAssignableFrom(property.DeclaringType)
					  ? property.Name == "Id2"
					  : defaultFindIdentityProperty(property);

				store.Conventions.FindTypeTagName = type =>
				                                    typeof (IDomainObject).IsAssignableFrom(type)
				                                    	? "domainobjects"
				                                    	: DocumentConvention.DefaultTypeTagName(type);

				using (var session = store.OpenSession())
				{
					var domainObject = new DomainObject();
					session.Store(domainObject);
					var domainObject2 = new DomainObject();
					session.Store(domainObject2);
					session.SaveChanges();
					id = domainObject.Id2;
				}
				var matchingDomainObjects = store.OpenSession().Query<IDomainObject>().Where(_ => _.Id2 == id).ToList();
				Assert.Equal(matchingDomainObjects.Count, 1);
			}


		}
	}
}
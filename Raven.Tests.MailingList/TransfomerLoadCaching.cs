using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class TransformerLoadCaching : RavenTest
	{
		public class Contact
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class ContactDto
		{
			public string ContactId { get; set; }
			public string ContactName { get; set; }
		}

		public class ContactTransformer : AbstractTransformerCreationTask<Contact>
		{
			public ContactTransformer()
			{
				TransformResults = contacts => from c in contacts
											   select new
											   {
												   ContactId = c.Id,
												   ContactName = c.Name
											   };
			}
		}

		 [Fact]
		 public void ShouldNotCacheStaleData()
		 {
			 using (var store = NewRemoteDocumentStore())
			 {
				 new ContactTransformer().Execute(store);

				 using (var session = store.OpenSession())
				 {
					 session.Store(new Contact{Name = "oren"});
					 session.SaveChanges();
				 }

				 using (var session = store.OpenSession())
				 {
					 var x = session.Load<ContactTransformer, ContactDto>("contacts/1");

					 Assert.Equal("oren", x.ContactName);
				 }


				 using (var session = store.OpenSession())
				 {
					 session.Load<Contact>(1).Name = "ayende";
					 session.SaveChanges();
				 }

				 using (var session = store.OpenSession())
				 {
					 var x = session.Load<ContactTransformer, ContactDto>("contacts/1");

					 Assert.Equal("ayende", x.ContactName);
				 }
			 }
		 }
	}
}
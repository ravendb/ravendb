using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class IndexWithSubProperty
	{
		[Fact]
		public void IndexWithSubPropertyReturnAs_Property_SubProperty()
		{
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				store.OpenSession();
				var index = new ContactIndex
				{
					Conventions = new DocumentConvention()
				};

				var result = index.CreateIndexDefinition();

				Assert.True(result.Stores.ContainsKey("PrimaryEmail_Email"));
				Assert.True(result.Indexes.ContainsKey("PrimaryEmail_Email"));
				Assert.True(result.Analyzers.ContainsKey("PrimaryEmail_Email"));
				Assert.True(result.Stores.ContainsKey("String_Store"));
				Assert.True(result.Indexes.ContainsKey("String_Index"));
				Assert.True(result.Analyzers.ContainsKey("String_Analyzer"));
			}
		}
	}

	public class ContactIndex : AbstractIndexCreationTask<Contact>
	{
		public ContactIndex()
		{
			Map = contacts =>
				from contact in contacts
				select new
				 {
					 contact.FirstName,
					 PrimaryEmail_EmailAddress = contact.PrimaryEmail.Email,
				 };

			Store("String_Store", FieldStorage.Yes);
			Store(x => x.PrimaryEmail.Email, FieldStorage.Yes);
			Index(x => x.PrimaryEmail.Email, FieldIndexing.Analyzed);
			Index("String_Index", FieldIndexing.Analyzed);
			Analyze(x => x.PrimaryEmail.Email, "SimpleAnalyzer");
			Analyze("String_Analyzer", "SnowballAnalyzer");
		}
	}

	public class Contact
	{
		public string Id { get; set; }
		public string FirstName { get; set; }
		public string Surname { get; set; }
		public EmailAddress PrimaryEmail { get; set; }
	}

	public class EmailAddress
	{
		public string Email { get; set; }
	}
}
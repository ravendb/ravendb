using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Indexes
{
	public class IndexWithSubProperty : RavenTest
	{
		[Fact]
		public void IndexWithSubPropertyReturnAs_Property_SubProperty()
		{
			var index = new ContactIndex
			{
				Conventions = new DocumentConvention()
			};
			var indexDefinition = index.CreateIndexDefinition();

			Assert.True(indexDefinition.Stores.ContainsKey("PrimaryEmail_Email"));
			Assert.True(indexDefinition.Indexes.ContainsKey("PrimaryEmail_Email"));
			Assert.True(indexDefinition.Analyzers.ContainsKey("PrimaryEmail_Email"));
			Assert.True(indexDefinition.Stores.ContainsKey("String_Store"));
			Assert.True(indexDefinition.Indexes.ContainsKey("String_Index"));
			Assert.True(indexDefinition.Analyzers.ContainsKey("String_Analyzer"));
		}

		public class ContactIndex : AbstractIndexCreationTask<Contact>
		{
			public ContactIndex()
			{
				Map = contacts => from contact in contacts
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
}
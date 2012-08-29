using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class IndexWithWhere : RavenTest
	{
		public class Document
		{
			public string Title { get; set; }

			public string Description { get; set; }

			public bool IsDeleted { get; set; }
		}

		public class Index_ByDescriptionAndTitle : AbstractIndexCreationTask<Document>
		{
			public Index_ByDescriptionAndTitle()
			{
				Map = docs => from doc in docs
							  where doc.Title == "dfsdfsfd" 
							  select new {doc.Description, doc.Title};
			}					  
		}

		public class Index_ByDescriptionAndTitle2  : AbstractIndexCreationTask<Document>
		{
			public Index_ByDescriptionAndTitle2()
			{
				Map = docs => from doc in docs
				              where
				              	doc.IsDeleted == false
				              select new {doc.Description, doc.Title};
			}
		}
		
		[Fact]
		public void CanCreateIndeX()
		{
			using(var store = NewDocumentStore())
			{
				new Index_ByDescriptionAndTitle().Execute(store);

				var indexDefinition = store.DatabaseCommands.GetIndex("Index/ByDescriptionAndTitle");
				Assert.Equal(@"docs.Documents.Where(doc => doc.Title == ""dfsdfsfd"").Select(doc => new {
    Description = doc.Description,
    Title = doc.Title
})", indexDefinition.Map);
			}	
		}

		[Fact]
		public void CanCreateIndeX2()
		{
			using (var store = NewDocumentStore())
			{
				new Index_ByDescriptionAndTitle2().Execute(store);

				var indexDefinition = store.DatabaseCommands.GetIndex("Index/ByDescriptionAndTitle2");
				Assert.Equal(@"docs.Documents.Where(doc => doc.IsDeleted == false).Select(doc => new {
    Description = doc.Description,
    Title = doc.Title
})", indexDefinition.Map);
			}
		}	 
	}
}
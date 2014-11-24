using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Ronne : RavenTest
	{
		public class Index : AbstractMultiMapIndexCreationTask
		{
			public Index()
			{
				AddMap<Sermon>(items => from x in items
										select new 
										{
											Content = new string[] { x.Description, x.Series, x.Speaker, x.Title }.Union(x.Tags)
										});
			}
		}

		[Fact]
		public void CanCreateIndexWithUnion()
		{
			using(var store = NewDocumentStore())
			{
				new Index().Execute(store);
			}
		}


		public class Sermon
		{
			public string Description, Series, Speaker, Title;
			public string[] Tags;
		}
	}

}
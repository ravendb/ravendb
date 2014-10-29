using System.Linq;
using Raven.Client.Embedded;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class RenamedProperty : RavenTest
	{
		[Fact]
		public void OrderByWithAttributeShouldStillWork()
		{
			using (var store = NewDocumentStore())
			{
				const int count = 1000;

				using (var session = store.OpenSession())
				{
					for (var i = 0; i < count; i++)
					{
						var model = new MyClass
						{
							ThisWontWork = i,
							ThisWillWork = i
						};
						session.Store(model);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var orderedWithoutAttribute = session.Query<MyClass>().OrderBy(x => x.ThisWillWork).Take(count).ToList();
					var orderedWithAttribute = session.Query<MyClass>().OrderByDescending(x => x.ThisWontWork).Take(count).ToList();

					Assert.Equal(count, orderedWithoutAttribute.Count);
					Assert.Equal(count, orderedWithAttribute.Count);

					for (var i = 1; i <= count; i++)
					{
						Assert.Equal(orderedWithoutAttribute[i - 1].ThisWontWork, orderedWithAttribute[count - i].ThisWontWork);
					}
				}
			}
		}

		public class MyClass
		{
			[JsonProperty("whoops")]
			public long ThisWontWork { get; set; }

			public long ThisWillWork { get; set; }
		}
	}
}
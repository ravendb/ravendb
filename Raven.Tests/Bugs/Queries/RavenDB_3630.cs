using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class RavenDB_3630: RavenTest
	{
		[Fact]
		public void CanQueryNotContainsOnEnum()
		{
			using (
			   var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					
					session.Store(new Issue3630(){Name="item 1",Enums = new List<Issue3630.MyEnum>(){Issue3630.MyEnum.IsGood,Issue3630.MyEnum.IsGood,Issue3630.MyEnum.IsGood,Issue3630.MyEnum.IsGood}});
					session.Store(new Issue3630() { Name = "item 2", Enums = new List<Issue3630.MyEnum>() { Issue3630.MyEnum.IsGood, Issue3630.MyEnum.IsBad, Issue3630.MyEnum.IsGood, Issue3630.MyEnum.IsGood } });
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var res = session.Query<Issue3630>().Single(item => !item.Enums.Contains(Issue3630.MyEnum.IsBad));
					Assert.Equal(res.Name, "item 1");
				}
			}
		}
	}
	public class Issue3630
	{
		public enum MyEnum
		{
			IsGood,
			IsBad
		}
		public string Name { get; set; }
		public IEnumerable<MyEnum> Enums { get; set; }
	}
}

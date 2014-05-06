using ICSharpCode.NRefactory.CSharp;
using Raven.Database.Linq.Ast;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.QueryOptimizer
{
	public class LinqToQueryParameters : NoDisposalNeeded
	{
		[Fact]
		public void SimpleProperties()
		{
			var qp = Translate("from doc in docs select new { doc.Name }");

			Assert.Equal(new[] { "Name" }, qp);
		}

		[Fact]
		public void NestedProperties()
		{
			var qp = Translate("from doc in docs select new { doc.User.Name }");

			Assert.Equal(new[] { "User.Name" }, qp);
		}

		[Fact]
		public void SimplePropertiesWithNamedArgument()
		{
			var qp = Translate("from doc in docs select new { N2 = doc.Name }");

			Assert.Equal(new[] { "Name" }, qp);
		}

		[Fact]
		public void NestedPropertiesWithNamedArgument()
		{
			var qp = Translate("from doc in docs select new { UserName = doc.User.Name }");

			Assert.Equal(new[] { "User.Name" }, qp);
		}

		[Fact]
		public void UsingLet()
		{
			var qp = Translate("from doc in docs let user = doc.User select new { user.Name }");

			Assert.Equal(new[] { "User.Name" }, qp);
		}

		[Fact]
		public void UsingSelectMany()
		{
			var qp = Translate("from doc in docs from item in doc.Items select new { item.Name }");

			Assert.Equal(new[] { "Items,Name" }, qp);
		}


		[Fact]
		public void UsingSelectMany2()
		{
			var qp = Translate(@"from p in docs.Posts
from t in p.Tags
select new { t.Name }");

			Assert.Equal(new[] { "Tags,Name" }, qp);
		}


		[Fact]
		public void UsingSelectManyWithMultipleValues()
		{
			var qp = Translate(@"from p in docs.Posts
from t in p.Tags
select new { TagsName = t.Name,  Name = p.Name }");

			Assert.Equal(new[] { "Tags,Name", "Name" }, qp);
		}


		[Fact]
		public void UsingSelectManyWithMultipleValuesUsingLet()
		{
			var qp = Translate(@"from p in docs.Posts
let user = p.User
from t in p.Tags
select new { TagsName = t.Name,  Name = p.Name, UserName = user.Name }");

			Assert.Equal(new[] { "Tags,Name", "Name", "User.Name" }, qp);
		}

		private static string[] Translate(string query)
		{
			var parser = new CSharpParser();

			var block = parser.ParseExpression(query);

			var captureQueryParameterNamesVisitor = new CaptureQueryParameterNamesVisitor();
			block.AcceptVisitor(captureQueryParameterNamesVisitor, null);
			return captureQueryParameterNamesVisitor.QueryParameters.ToArray();
		}
	}
}
using System;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Querying
{
	public class UsingStronglyTypedDocumentQuery
	{
		private IDocumentQuery<IndexedUser> CreateUserQuery()
		{
			return new DocumentQuery<IndexedUser>(null, null, null, "IndexName", null, null, null);
		}

		[Fact]
		public void WhereEqualsSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereEquals("Name", "ayende", false).ToString(), 
				CreateUserQuery().WhereEquals(x => x.Name, "ayende", false).ToString());
			Assert.Equal(CreateUserQuery().WhereEquals("Name", "ayende").ToString(), CreateUserQuery()
				.WhereEquals(x => x.Name, "ayende").ToString());
		}

		[Fact]
		public void WhereInSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereIn("Name", new[] { "ayende", "tobias" }).ToString(),
				CreateUserQuery().WhereIn(x => x.Name, new[] { "ayende", "tobias" }).ToString());
		}
		
		[Fact]
		public void WhereStartsWithSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereStartsWith("Name", "ayende").ToString(),
				CreateUserQuery().WhereStartsWith(x => x.Name, "ayende").ToString());
		}

		[Fact]
		public void WhereEndsWithSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereEndsWith("Name", "ayende").ToString(),
				CreateUserQuery().WhereEndsWith(x => x.Name, "ayende").ToString());
		}

		[Fact]
		public void WhereBetweenSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereBetween("Name", "ayende", "zaphod").ToString(),
				CreateUserQuery().WhereBetween(x => x.Name, "ayende", "zaphod").ToString());
		}

		[Fact]
		public void WhereBetweenOrEqualSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereBetweenOrEqual("Name", "ayende", "zaphod").ToString(),
				CreateUserQuery().WhereBetweenOrEqual(x => x.Name, "ayende", "zaphod").ToString());
		}

		[Fact]
		public void WhereGreaterThanSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereGreaterThan("Birthday", new DateTime(1970,01,01)).ToString(),
				CreateUserQuery().WhereGreaterThan(x => x.Birthday, new DateTime(1970, 01, 01)).ToString());
		}
		
		[Fact]
		public void WhereGreaterThanOrEqualSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereGreaterThanOrEqual("Birthday", new DateTime(1970, 01, 01)).ToString(),
				CreateUserQuery().WhereGreaterThanOrEqual(x => x.Birthday, new DateTime(1970, 01, 01)).ToString());
		}

		[Fact]
		public void WhereLessThanSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereLessThan("Birthday", new DateTime(1970, 01, 01)).ToString(),
				CreateUserQuery().WhereLessThan(x => x.Birthday, new DateTime(1970, 01, 01)).ToString());
		}

		[Fact]
		public void WhereLessThanOrEqualSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().WhereLessThanOrEqual("Birthday", new DateTime(1970, 01, 01)).ToString(),
				CreateUserQuery().WhereLessThanOrEqual(x => x.Birthday, new DateTime(1970, 01, 01)).ToString());
		}

		[Fact]
		public void SearchSameAsUntypedCounterpart()
		{
			Assert.Equal(CreateUserQuery().Search("Name", "ayende").ToString(),
				CreateUserQuery().Search(x => x.Name, "ayende").ToString());
		}
	
		[Fact]
		public void CanUseStronglyTypedAddOrder()
		{
			CreateUserQuery().AddOrder(x => x.Birthday, false);
		}

		[Fact]
		public void CanUseStronglyTypedOrderBy()
		{
			CreateUserQuery().OrderBy(x => x.Birthday);
		}

		[Fact]
		public void CanUseStronglyTypedSearch()
		{
			CreateUserQuery().Search(x => x.Birthday, "1975");
		}
	
		[Fact]
		public void CanUseStronglyTypedGroupBy()
		{
			CreateUserQuery().GroupBy(AggregationOperation.None, x => x.Birthday);
		}
	}
}
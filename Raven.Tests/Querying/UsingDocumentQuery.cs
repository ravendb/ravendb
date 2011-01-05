//-----------------------------------------------------------------------
// <copyright file="UsingDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Xunit;
using Raven.Client.Document;

namespace Raven.Tests.Querying
{
    public class UsingDocumentQuery
	{
		[Fact]
		public void CanUnderstandSimpleEquality()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereEquals("Name", "ayende",false);

			Assert.Equal("Name:[[ayende]]", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleEqualityWithVariable()
		{
			var ayende = "ayende" + 1;
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereEquals("Name", ayende, false);
			Assert.Equal("Name:[[ayende1]]", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleContains()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereContains("Name", "ayende");
			Assert.Equal("Name:ayende", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleContainsWithVariable()
		{
			var ayende = "ayende" + 1;
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereContains("Name", ayende);
			Assert.Equal("Name:ayende1", q.ToString());
		}

		[Fact]
		public void NoOpShouldProduceEmptyString()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null);
			Assert.Equal("", q.ToString());
		}

		[Fact]
		public void CanUnderstandAnd()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereContains("Name", "ayende")
				.AndAlso()
				.WhereContains("Email", "ayende@ayende.com");
			Assert.Equal("Name:ayende AND Email:ayende@ayende.com", q.ToString());
		}

		[Fact]
		public void CanUnderstandOr()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereContains("Name", "ayende")
				.OrElse()
				.WhereContains("Email", "ayende@ayende.com");
			Assert.Equal("Name:ayende OR Email:ayende@ayende.com", q.ToString());
		}

		[Fact]
		public void CanUnderstandLessThan()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereLessThan("Birthday", new DateTime(2010, 05, 15));
			Assert.Equal("Birthday:{* TO 20100515000000000}", q.ToString());
		}

		[Fact]
		public void CanUnderstandEqualOnDate()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereEquals("Birthday", new DateTime(2010, 05, 15));
			Assert.Equal("Birthday:20100515000000000", q.ToString());
		}

		[Fact]
		public void CanUnderstandLessThanOrEqual()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereLessThanOrEqual("Birthday", new DateTime(2010, 05, 15));
			Assert.Equal("Birthday:[* TO 20100515000000000]", q.ToString());
		}

		[Fact]
		public void CanUnderstandGreaterThan()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereGreaterThan("Birthday", new DateTime(2010, 05, 15));
			Assert.Equal("Birthday:{20100515000000000 TO NULL}", q.ToString());
		}

		[Fact]
		public void CanUnderstandGreaterThanOrEqual()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereGreaterThanOrEqual("Birthday", new DateTime(2010, 05, 15));
			Assert.Equal("Birthday:[20100515000000000 TO NULL]", q.ToString());
		}

		[Fact]
		public void CanUnderstandProjectionOfSingleField()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereGreaterThanOrEqual("Birthday", new DateTime(2010, 05, 15))
				.SelectFields<IndexedUser>("Name") as DocumentQuery<IndexedUser>;
			string fields = q.GetProjectionFields().Any() ?
				"<" + String.Join(", ", q.GetProjectionFields().ToArray()) + ">: " : "";
			Assert.Equal("<Name>: Birthday:[20100515000000000 TO NULL]", fields + q.ToString());
		}

		[Fact]
		public void CanUnderstandProjectionOfMultipleFields()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereGreaterThanOrEqual("Birthday", new DateTime(2010, 05, 15))
				.SelectFields<IndexedUser>("Name", "Age") as DocumentQuery<IndexedUser>;
			string fields = q.GetProjectionFields().Any() ?
				"<" + String.Join(", ", q.GetProjectionFields().ToArray()) + ">: " : "";
			Assert.Equal("<Name, Age>: Birthday:[20100515000000000 TO NULL]", fields + q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleEqualityOnInt()
		{
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereEquals("Age", 3, false);
			Assert.Equal("Age:3", q.ToString());
		}

		[Fact]
		public void CanUnderstandGreaterThanOnInt()
		{
			// should DocumentQuery<T> understand how to generate range field names?
			var q = new DocumentQuery<IndexedUser>(null, null, "IndexName", null, null)
				.WhereGreaterThan("Age_Range", 3);
			Assert.Equal("Age_Range:{0x00000003 TO NULL}", q.ToString());
		}
	}
}

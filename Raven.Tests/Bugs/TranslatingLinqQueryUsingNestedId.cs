//-----------------------------------------------------------------------
// <copyright file="TranslatingLinqQueryUsingNestedId.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class TranslatingLinqQueryUsingNestedId : RavenTest
	{
		[Fact]
		public void Id_on_member_should_not_be_converted_to_document_id()
		{
			var generated = new IndexDefinitionBuilder<SubCategory>
			{
				Map = subs => from subCategory in subs
							  select new
							  {
								  CategoryId = subCategory.Id,
								  SubCategoryId = subCategory.Parent.Id
							  }
			}.ToIndexDefinition(new DocumentConvention());
			
			Assert.Contains("CategoryId = subCategory.__document_id", generated.Map);
			Assert.Contains("SubCategoryId = subCategory.Parent.Id", generated.Map);
		}

		#region Nested type: Category

		public class Category
		{
			public string Id { get; set; }
		}

		#endregion

		#region Nested type: SubCategory

		public class SubCategory
		{
			public string Id { get; set; }
			public Category Parent { get; set; }
		}

		#endregion
	}
}
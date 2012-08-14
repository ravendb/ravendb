using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class TranslatingLinqQueriesToIndexes
	{
		[Fact]
		public void WillTranslateReferenceToIdTo__docuent_id()
		{
			Expression<Func<IEnumerable<Nestable>, IEnumerable>> map = nests => from nestable in nests
																				select new { nestable.Id };
			var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Nestable, Nestable>(map, new DocumentConvention(), "docs", true);
			Assert.Contains("Id = nestable.__document_id", code);
		}

		[Fact]
		public void WillNotTranslateIdTo__document_idIfNotOnRootEntity()
		{
			Expression<Func<IEnumerable<Nestable>, IEnumerable>> map = nests => from nestable in nests
																				from child in nestable.Children
																				select new { child.Id };
			var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Nestable, Nestable>(map, new DocumentConvention(), "docs", true);
			Assert.Contains("Id = child.Id", code);
		}

		[Fact]
		public void WillTranslateProperlyBothRootAndChild()
		{
			Expression<Func<IEnumerable<Nestable>, IEnumerable>> map = nests => from nestable in nests
																				from child in nestable.Children
																				select new { child.Id, Id2 = nestable.Id };
			var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Nestable, Nestable>(map, new DocumentConvention(), "docs", true);
			Assert.Contains("Id = child.Id", code);
			Assert.Contains("Id2 = nestable.__document_id", code);
		}

		[Fact]
		public void WillTranslateAnonymousArray()
		{
			Expression<Func<IEnumerable<Nestable>, IEnumerable>> map = nests => from nestable in nests
																				let elements = new[] { new { Id = nestable.Id }, new { Id = nestable.Id } }
																				from element in elements
																				select new { Id = element.Id };
			var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Nestable, Nestable>(map, new DocumentConvention(), "docs", true);
			Assert.Contains("new[]", code);
		}


		public class Nestable
		{
			public string Id { get; set; }
			public Nestable[] Children { get; set; }
		}
	}
}
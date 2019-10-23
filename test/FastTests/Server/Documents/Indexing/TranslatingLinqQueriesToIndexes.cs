using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Indexing
{
    public class TranslatingLinqQueriesToIndexes : NoDisposalNeeded
    {
        public TranslatingLinqQueriesToIndexes(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WillTranslateReferenceToIdTo__document_id()
        {
            Expression<Func<IEnumerable<Nestable>, IEnumerable>> map = nests => from nestable in nests
                select new { nestable.Id };
            var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Nestable, Nestable>(map, new DocumentConventions(), "docs", true);
            Assert.Contains("Id = Id(nestable)", code);
        }

        [Fact]
        public void WillNotTranslateIdTo__document_idIfNotOnRootEntity()
        {
            Expression<Func<IEnumerable<Nestable>, IEnumerable>> map = nests => from nestable in nests
                from child in nestable.Children
                select new { child.Id };
            var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Nestable, Nestable>(map, new DocumentConventions(), "docs", true);
            Assert.Contains("Id = child.Id", code);
        }

        [Fact]
        public void WillTranslateProperlyBothRootAndChild()
        {
            Expression<Func<IEnumerable<Nestable>, IEnumerable>> map = nests => from nestable in nests
                from child in nestable.Children
                select new { child.Id, Id2 = nestable.Id };
            var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Nestable, Nestable>(map, new DocumentConventions(), "docs", true);
            Assert.Contains("Id = child.Id", code);
            Assert.Contains("Id2 = Id(nestable)", code);
        }

        [Fact]
        public void WillTranslateAnonymousArray()
        {
            Expression<Func<IEnumerable<Nestable>, IEnumerable>> map = nests => from nestable in nests
                let elements = new[] { new { Id = nestable.Id }, new { Id = nestable.Id } }
                from element in elements
                select new { Id = element.Id };
            var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Nestable, Nestable>(map, new DocumentConventions(), "docs", true);
            Assert.Contains("new[]", code);
        }


        public class Nestable
        {
            public string Id { get; set; }
            public Nestable[] Children { get; set; }
        }
    }
}

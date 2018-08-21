using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDb3973 : RavenTestBase
    {
        [Fact]
        public void VerifyNegateQueryOptimization()
        {
            using (var store = GetDocumentStore())
            {
                CreateEntities(store);
                PerfromQuery(store);
            }
        }

        private class Entity3973
        {
            public int OrganizationId;
            public long HistoryCode;
            public int CaseId;
        }

        private class EntityIndex : AbstractIndexCreationTask<Entity3973>
        {
            public EntityIndex()
            {
                Map = entities => from e in entities
                                  select new
                                  {
                                      e.OrganizationId,
                                      e.HistoryCode,
                                      e.CaseId
                                  };
            }
        }

        private void PerfromQuery(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                store.ExecuteIndex(new EntityIndex());
                TheIssueQuery(session);
            }
        }

        private static void TheIssueQuery(IDocumentSession session)
        {
            var expressions = new List<Tuple<Expression<Func<Entity3973, bool>>,
                Func<List<Entity3973>, bool>, // Expected results from DB - true if problem
                string>>(); // Expeceted Expression WITH optimization

            // && != &&
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 && e.HistoryCode == 2 && e.CaseId != 5,
                r => r.Count != 3 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 6 }) == false,
                "from index 'EntityIndex' where ((OrganizationId = $p0 and HistoryCode = $p1) and CaseId != $p2)"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 && e.CaseId != 5 && e.HistoryCode == 2,
                r => r.Count != 3 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 6 }) == false,
                "from index 'EntityIndex' where ((OrganizationId = $p0 and CaseId != $p1)) and HistoryCode = $p2"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 5 && e.OrganizationId == 1 && e.HistoryCode == 2,
                r => r.Count != 3 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 6 }) == false,
                "from index 'EntityIndex' where ((CaseId != $p0 and OrganizationId = $p1)) and HistoryCode = $p2"));

            // || != &&
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 || e.HistoryCode == 2 && e.CaseId != 5,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "from index 'EntityIndex' where OrganizationId = $p0 or ((HistoryCode = $p1 and CaseId != $p2))"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 || e.CaseId != 5 && e.HistoryCode == 2,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "from index 'EntityIndex' where OrganizationId = $p0 or ((CaseId != $p1 and HistoryCode = $p2))"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 5 || e.OrganizationId == 1 && e.HistoryCode == 2,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "from index 'EntityIndex' where CaseId != $p0 or (OrganizationId = $p1 and HistoryCode = $p2)"));

            // && != ||
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 && e.HistoryCode == 2 || e.CaseId != 5,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "from index 'EntityIndex' where (OrganizationId = $p0 and HistoryCode = $p1) or CaseId != $p2"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 && e.CaseId != 5 || e.HistoryCode == 2,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "from index 'EntityIndex' where ((OrganizationId = $p0 and CaseId != $p1)) or HistoryCode = $p2"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 5 && e.OrganizationId == 1 || e.HistoryCode == 2,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "from index 'EntityIndex' where ((CaseId != $p0 and OrganizationId = $p1)) or HistoryCode = $p2"));

            // Other variations:
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => (e.OrganizationId == 1 && e.CaseId == 3) && (e.CaseId != 5 || e.HistoryCode == 2),
                r => r.Count != 1 || r[0].CaseId != 3,
                "from index 'EntityIndex' where (OrganizationId = $p0 and CaseId = $p1) and (CaseId != $p2 or HistoryCode = $p3)"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => (e.OrganizationId == 1 || e.CaseId == 3) && (e.CaseId != 5 && e.HistoryCode == 2),
                r => r.Count != 3 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 6 }) == false,
                "from index 'EntityIndex' where (OrganizationId = $p0 or CaseId = $p1) and ((CaseId != $p2 and HistoryCode = $p3))"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 3 && e.CaseId != 5,
                r => r.Count != 2 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 4, 6 }) == false,
                "from index 'EntityIndex' where (CaseId != $p0 and CaseId != $p1)"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 3 || e.CaseId != 5,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "from index 'EntityIndex' where CaseId != $p0 or CaseId != $p1"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 3 && e.CaseId != 5 && e.CaseId != 6,
                r => r.Count != 1 || r[0].CaseId != 4,
                "from index 'EntityIndex' where (((CaseId != $p0 and CaseId != $p1)) and CaseId != $p2)"));

            for (var i = 0; i < expressions.Count; i++)
            {
                var x = expressions[i];
                var queryExpOptimized = session.Query<Entity3973, EntityIndex>()
                    .Where(x.Item1);
                if (queryExpOptimized.ToString() == x.Item3)
                    continue;

            }

            for (int i = 0; i < expressions.Count; i++)
            {
                var x = expressions[i];

                var queryExpOptimized = session.Query<Entity3973, EntityIndex>()
                    .Where(x.Item1);

                // if not expected results then might be because of a bad isNotEqualCheckBoundsToAndAlso optimization
                Assert.Equal(x.Item3, queryExpOptimized.ToString());

                var resOptimized = session.Query<Entity3973, EntityIndex>()
                    .Where(x.Item1)
                    .Customize(y => y.WaitForNonStaleResults())
                    .ToList();

                var equals = x.Item2(resOptimized);
                Assert.False(equals);
            }
        }

        private static void CreateEntities(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                for (var x = 3; x < 7; x++)
                {
                    var entity = new Entity3973()
                    {
                        OrganizationId = 1,
                        HistoryCode = 2,
                        CaseId = x
                    };
                    session.Store(entity);
                }
                session.SaveChanges();
            }
        }
    }
}

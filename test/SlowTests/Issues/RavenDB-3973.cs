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
                "FROM INDEX 'EntityIndex' WHERE ((OrganizationId = :p0 AND HistoryCode = :p1) AND NOT CaseId = :p2)"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 && e.CaseId != 5 && e.HistoryCode == 2,
                r => r.Count != 3 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE ((OrganizationId = :p0 AND NOT CaseId = :p1)) AND HistoryCode = :p2"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 5 && e.OrganizationId == 1 && e.HistoryCode == 2,
                r => r.Count != 3 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE ((exists(CaseId) AND NOT CaseId = :p0 AND OrganizationId = :p1)) AND HistoryCode = :p2"));

            // || != &&
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 || e.HistoryCode == 2 && e.CaseId != 5,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE OrganizationId = :p0 OR ((HistoryCode = :p1 AND NOT CaseId = :p2))"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 || e.CaseId != 5 && e.HistoryCode == 2,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE OrganizationId = :p0 OR ((exists(CaseId) AND NOT CaseId = :p1 AND HistoryCode = :p2))"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 5 || e.OrganizationId == 1 && e.HistoryCode == 2,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE (exists(CaseId) AND NOT CaseId = :p0) OR (OrganizationId = :p1 AND HistoryCode = :p2)"));

            // && != ||
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 && e.HistoryCode == 2 || e.CaseId != 5,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE (OrganizationId = :p0 AND HistoryCode = :p1) OR (exists(CaseId) AND NOT CaseId = :p2)"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.OrganizationId == 1 && e.CaseId != 5 || e.HistoryCode == 2,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE ((OrganizationId = :p0 AND NOT CaseId = :p1)) OR HistoryCode = :p2"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 5 && e.OrganizationId == 1 || e.HistoryCode == 2,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE ((exists(CaseId) AND NOT CaseId = :p0 AND OrganizationId = :p1)) OR HistoryCode = :p2"));

            // Other variations:
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => (e.OrganizationId == 1 && e.CaseId == 3) && (e.CaseId != 5 || e.HistoryCode == 2),
                r => r.Count != 1 || r[0].CaseId != 3,
                "FROM INDEX 'EntityIndex' WHERE (OrganizationId = :p0 AND CaseId = :p1) AND ((exists(CaseId) AND NOT CaseId = :p2) OR HistoryCode = :p3)"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => (e.OrganizationId == 1 || e.CaseId == 3) && (e.CaseId != 5 && e.HistoryCode == 2),
                r => r.Count != 3 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE (OrganizationId = :p0 OR CaseId = :p1) AND ((exists(CaseId) AND NOT CaseId = :p2 AND HistoryCode = :p3))"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 3 && e.CaseId != 5,
                r => r.Count != 2 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 4, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE ((exists(CaseId) AND NOT CaseId = :p0) AND NOT CaseId = :p1)"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 3 || e.CaseId != 5,
                r => r.Count != 4 || r.Select(x => x.CaseId).ToHashSet().SetEquals(new[] { 3, 4, 5, 6 }) == false,
                "FROM INDEX 'EntityIndex' WHERE (exists(CaseId) AND NOT CaseId = :p0) OR (exists(CaseId) AND NOT CaseId = :p1)"));
            expressions.Add(new Tuple<Expression<Func<Entity3973, bool>>, Func<List<Entity3973>, bool>, string>(e => e.CaseId != 3 && e.CaseId != 5 && e.CaseId != 6,
                r => r.Count != 1 || r[0].CaseId != 4,
                "FROM INDEX 'EntityIndex' WHERE ((((exists(CaseId) AND NOT CaseId = :p0) AND NOT CaseId = :p1)) AND NOT CaseId = :p2)"));

            foreach (var x in expressions)
            {
                var queryExpOptimized = session.Query<Entity3973, EntityIndex>()
                    .Where(x.Item1);
                if(queryExpOptimized.ToString() == x.Item3)
                    continue;
                
                Console.WriteLine(queryExpOptimized.ToString());
            }

            expressions.ForEach(x =>
            {
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
            });
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

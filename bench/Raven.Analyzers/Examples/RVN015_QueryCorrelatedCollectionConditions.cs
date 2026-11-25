// Expected diagnostic: RVN015 - "Correlated conditions over a nested collection in a dynamic query"
// The auto index behind a dynamic query flattens Members into Members[].UserId and
// Members[].Role, so the two conditions are matched against different members and the
// query returns projects where no single member satisfies both.
//
// Fix: query a static index whose map fans out one entry per member.
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Examples;

public static class RVN015_QueryCorrelatedCollectionConditions
{
    public static List<RVN015_Project> BadExample(IDocumentSession session, string userId)
    {
        // warning RVN015: 'Any' over 'p.Members' combines conditions that the auto index matches
        //   against different elements.
        return session.Query<RVN015_Project>()
            .Where(p => p.Members.Any(m => m.UserId == userId && m.Role == RVN015_Role.Contributor))
            .Take(50)
            .ToList();
    }

    public static List<RVN015_Project> GoodExample(IDocumentSession session, string userId)
    {
        // Correct: the fan-out index emits one entry per member, so both conditions hit the same one
        return session.Query<RVN015_Projects_ByMember.Result, RVN015_Projects_ByMember>()
            .Where(r => r.UserId == userId && r.Role == RVN015_Role.Contributor)
            .OfType<RVN015_Project>()
            .Take(50)
            .ToList();
    }

    public static List<RVN015_Project> UncorrelatedExample(IDocumentSession session, string userId)
    {
        // Not flagged: two separate Any calls state the uncorrelated matching explicitly
        return session.Query<RVN015_Project>()
            .Where(p => p.Members.Any(m => m.UserId == userId) && p.Members.Any(m => m.Role == RVN015_Role.Contributor))
            .Take(50)
            .ToList();
    }
}

#pragma warning disable RVN014
public class RVN015_Projects_ByMember : AbstractIndexCreationTask<RVN015_Project>
{
    public class Result
    {
        public string UserId { get; set; } = default!;
        public RVN015_Role Role { get; set; }
    }

    public RVN015_Projects_ByMember()
    {
        Map = projects => from p in projects
                          from m in p.Members
                          select new { m.UserId, m.Role };
    }
}
#pragma warning restore RVN014

public enum RVN015_Role { Viewer, Contributor }

public class RVN015_Member
{
    public string UserId { get; set; } = default!;
    public RVN015_Role Role { get; set; }
}

public class RVN015_Project
{
    public string Id { get; set; } = default!;
    public List<RVN015_Member> Members { get; set; } = new();
}

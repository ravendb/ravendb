// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3417.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web.Http.Controllers;
using Raven.Database.Counters.Controllers;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.TimeSeries.Controllers;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{

    public class PossibleConflict
    {
        public MethodInfo MethodWithWildCard;
        public MethodInfo MethodWithOutWildCard;
        public string Verb;
    }

    public class RavenDB_5857
    {
        private readonly MethodInfo[] allRoutes;
        private readonly Dictionary<object, List<MethodInfo>> routesViaVerb;

        private readonly MethodInfo[] exclusions = new[]
        {
            typeof(IndexController).GetMethod("SetPriority"),
            typeof(AdminDatabasesController).GetMethod("DatabaseBatchToggleDisable"),
            typeof(StaticController).GetMethod("StaticGet", new Type[0]),
            typeof(SilverlightController).GetMethod("SilverlightEnsureStartup"),
            typeof(IndexController).GetMethod("IndexUpdateLastQueried"),

            typeof(AdminTimeSeriesController).GetMethod("BatchDelete"), // ignoring as we don't support ts, cs,
            typeof(AdminTimeSeriesController).GetMethod("ToggleDisable"),// ignoring as we don't support ts, cs,
            typeof(AdminCounterStorageController).GetMethod("BatchDelete"),// ignoring as we don't support ts, cs,
            typeof(AdminCounterStorageController).GetMethod("ToggleDisable")// ignoring as we don't support ts, cs,

        };

        public RavenDB_5857()
        {
            allRoutes = typeof(BaseDatabaseApiController)
               .Assembly
               .GetTypes()
               .SelectMany(t => t.GetMethods())
               .Where(x => x.GetCustomAttributes(typeof(RavenRouteAttribute), false).Length > 0)
               .ToArray();

            routesViaVerb = allRoutes
               .GroupBy(x => x.GetCustomAttributes(typeof(IActionHttpMethodProvider), true).First())
               .ToDictionary(x => x.Key, x => x.ToList());
        }

        [Fact]
        public void DetectPossibleIssuesWithCacheSystem()
        {
            var collisions = new List<PossibleConflict>();

            foreach (var methodWithWildCard in allRoutes)
            {
                foreach (var routeWithWildCard in methodWithWildCard.GetCustomAttributes<RavenRouteAttribute>().Where(x => x.Template.Contains("{*")))
                {
                    var httpVerb = methodWithWildCard.GetCustomAttributes(typeof(IActionHttpMethodProvider), true).First();

                    var prewildcardPrefix = routeWithWildCard.Template.Substring(0, routeWithWildCard.Template.IndexOf("{*", StringComparison.Ordinal));

                    var candidates = routesViaVerb[httpVerb]
                        .Where(m => m.GetCustomAttributes<RavenRouteAttribute>().Any(r => r.Template.StartsWith(prewildcardPrefix)))
                        .Where(x => x != methodWithWildCard)
                        .Where(x => !exclusions.Contains(x))
                        .Select(x => new PossibleConflict
                        {
                            MethodWithOutWildCard = x,
                            MethodWithWildCard = methodWithWildCard,
                            Verb = httpVerb.GetType().Name
                        });

                    collisions.AddRange(candidates);
                }
            }

            if (collisions.Count > 0)
            {
                var collisionsFormatted = collisions
                    .Select(collision => $"{collision.Verb}: {DescribeMethod(collision.MethodWithWildCard)} may conflict with: {DescribeMethod(collision.MethodWithOutWildCard)}")
                    .ToArray();
                throw new Exception(string.Join(Environment.NewLine, collisionsFormatted));
            }
        }

        private static string DescribeMethod(MethodInfo methodInfo)
        {

            return methodInfo.Name + "@" + methodInfo.DeclaringType.Name;
        }
    }
}

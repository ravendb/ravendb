// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1666.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Web.Http;
using Raven.Database.Counters.Controllers;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.FileSystem.Controllers;
using Raven.Tests.Common;

using Xunit;
using System.Linq;
using LogsController = Raven.Database.Server.Controllers.LogsController;

namespace Raven.Tests.Issues
{
    public class RavenDB_1666 : RavenTest
    {

        private readonly List<MethodInfo> ignoredMethods = new List<MethodInfo>();
        private readonly List<String> ignoredNamespaces = new List<string>(); 
        private void RegisterRouteForOnlySysDb<T>(Expression<Action<T>> a)
        {
            ignoredMethods.Add(((MethodCallExpression) a.Body).Method);
        }

        private void RegisterNoSysDbForControllersInThisNamespace<TController>()
        {
            ignoredNamespaces.Add(typeof(TController).Namespace);
        }

        [Fact]
        public void GenerateIdentityRemote()
        {
            using (var store = NewRemoteDocumentStore(databaseName:"identityRemote"))
            {
                Assert.Equal(1, store.DatabaseCommands.NextIdentityFor("chairs"));
                Assert.Equal(2, store.DatabaseCommands.NextIdentityFor("chairs"));
            }
        }

        /// <summary>
        /// This is meta test used for route analysis, when there is route for system db and route for non-system db is not present (or vice versa),
        /// then if controller is not present on ignore list throw an exception. 
        /// </summary>
        [Fact]
        public void CheckRoutes()
        {
            RegisterNoSysDbForControllersInThisNamespace<FilesController>();
            RegisterNoSysDbForControllersInThisNamespace<CountersController>();

            RegisterRouteForOnlySysDb<StudioTasksController>(a => a.GetLatestServerBuildVersion(false, 3000, 3999));
            RegisterRouteForOnlySysDb<AdminController>(a => a.Stats());
            RegisterRouteForOnlySysDb<AdminController>(a => a.OnAdminLogsConfig());
            RegisterRouteForOnlySysDb<AdminController>(a => a.OnAdminLogsFetch());
            RegisterRouteForOnlySysDb<AdminController>(a => a.IoTest());
            RegisterRouteForOnlySysDb<AdminController>(a => a.InfoPackage());
            RegisterRouteForOnlySysDb<AdminController>(a => a.Compact());
            RegisterRouteForOnlySysDb<AdminController>(a => a.Gc());
            RegisterRouteForOnlySysDb<AdminController>(a => a.LohCompaction());
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabasesGet(string.Empty));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabasesPut(string.Empty));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabasesDelete(string.Empty));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabasesBatchDelete());
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.ToggleDisable(string.Empty, false));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.ToggleDisable(string.Empty, true));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabaseBatchToggleDisable(false));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabaseBatchToggleDisable(true));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.ToggleIndexingDisable(null, false));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabaseToggleRejectClientsEnabled(string.Empty, false));
            RegisterRouteForOnlySysDb<DatabasesController>(a => a.Databases(false));
            RegisterRouteForOnlySysDb<DatabasesController>(a => a.Databases(true));
            RegisterRouteForOnlySysDb<FileSystemsController>(a => a.FileSystems(false));
            RegisterRouteForOnlySysDb<FileSystemsController>(a => a.FileSystems(true));
            RegisterRouteForOnlySysDb<DebugController>(a => a.Routes());
            RegisterRouteForOnlySysDb<HardRouteController>(a => a.FaviconGet());
            RegisterRouteForOnlySysDb<HardRouteController>(a => a.ClientaccessPolicyGet());
            RegisterRouteForOnlySysDb<HardRouteController>(a => a.RavenRoot());
            RegisterRouteForOnlySysDb<LicensingController>(a => a.LicenseStatusGet());
            RegisterRouteForOnlySysDb<LogsController>(a => a.LogsGet());
            RegisterRouteForOnlySysDb<OAuthController>(a => a.ApiKeyPost());
            RegisterRouteForOnlySysDb<PluginController>(a => a.PlugingsStatusGet());
            RegisterRouteForOnlySysDb<StudioController>(a => a.RavenUiGet(null));
            RegisterRouteForOnlySysDb<StudioController>(a => a.GetStudioFile(null));
            RegisterRouteForOnlySysDb<StudioTasksController>(a => a.GetNewEncryption(null));
            RegisterRouteForOnlySysDb<StudioTasksController>(a => a.IsBase64Key(null));
            RegisterRouteForOnlySysDb<SilverlightController>(a => a.SilverlightUi(null));
            RegisterRouteForOnlySysDb<SilverlightController>(a => a.SilverlightEnsureStartup());

            const string nonSystemDbPrefix = "databases/{databaseName}/";

            var routeMethods = typeof (RavenDbApiController).Assembly
                                                            .DefinedTypes
                                                            .Where(t=> ignoredNamespaces.Contains(t.Namespace) == false)
                                                            .SelectMany(t => t.GetMethods())
                                                            .Where(m => m.CustomAttributes.Any(a => a.AttributeType == typeof(RouteAttribute)));

            var result = new List<string>();


            foreach (var routeMethod in routeMethods.Where(m => !ignoredMethods.Contains(m)))
            {
                var routeAttributes = routeMethod.CustomAttributes.Where(a => a.AttributeType == typeof (RouteAttribute));
                var constructArgs = routeAttributes.Select(attr => attr.ConstructorArguments[0].Value).Cast<string>();
                var nonSystemAndSystemLookup = constructArgs.ToLookup(s => s.StartsWith(nonSystemDbPrefix));
                var nonSystemDbs = nonSystemAndSystemLookup[true].Select(s => s.Substring(nonSystemDbPrefix.Length));
                var systemDbs = nonSystemAndSystemLookup[false];

                // find symmetric difference
                var difference = SymmetricDifference(nonSystemDbs, systemDbs);

                if (difference.Any())
                {
                    result.Add(String.Format("Possible invalid route mapping in type: {0}, method = {1}. You can add this method to ignore list.", routeMethod.DeclaringType.FullName, routeMethod.Name));
                }

            }

            if (result.Any())
            {
                Assert.False(true, string.Join(Environment.NewLine, result));
            }


        } 

        private IEnumerable<T> SymmetricDifference<T>(IEnumerable<T> first, IEnumerable<T> second)
        {
            var result = new HashSet<T>(first);
            result.SymmetricExceptWith(second);
            return result;
        }
    }
}

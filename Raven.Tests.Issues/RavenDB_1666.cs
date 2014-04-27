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
using Raven.Client.RavenFS;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.RavenFS.Controllers;
using Raven.Tests.Common;

using Xunit;
using System.Linq;
using LogsController = Raven.Database.Server.Controllers.LogsController;

namespace Raven.Tests.Issues
{
    public class RavenDB_1666 : RavenTest
    {

        private readonly List<MethodInfo> ignoredMethods = new List<MethodInfo>();
        private void RegisterRouteForOnlySysDb<T>(Expression<Action<T>> a)
        {
            ignoredMethods.Add(((MethodCallExpression) a.Body).Method);
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
            
            RegisterRouteForOnlySysDb<AdminController>(a => a.Stats());
            RegisterRouteForOnlySysDb<AdminController>(a => a.Gc());
            RegisterRouteForOnlySysDb<AdminController>(a => a.LohCompaction());
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabasesGet(string.Empty));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabasePost(string.Empty));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabasesDelete(string.Empty));
            RegisterRouteForOnlySysDb<AdminDatabasesController>(a => a.DatabasesPut(string.Empty));
            RegisterRouteForOnlySysDb<DatabasesController>(a => a.Databases());
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
            RegisterRouteForOnlySysDb<SilverlightController>(a => a.SilverlightUi(string.Empty));
            RegisterRouteForOnlySysDb<ConfigController>(a => a.Get());
            RegisterRouteForOnlySysDb<ConfigController>(a => a.Get(string.Empty));
            RegisterRouteForOnlySysDb<ConfigController>(a => a.ConfigNamesStartingWith(string.Empty));
            RegisterRouteForOnlySysDb<ConfigController>(a => a.Put(string.Empty));
            RegisterRouteForOnlySysDb<ConfigController>(a => a.Delete(string.Empty));
            RegisterRouteForOnlySysDb<FilesController>(a => a.Get());
            RegisterRouteForOnlySysDb<FilesController>(a => a.Get(string.Empty));
            RegisterRouteForOnlySysDb<FilesController>(a => a.Head(string.Empty));
            RegisterRouteForOnlySysDb<FilesController>(a => a.Put(string.Empty, null));
            RegisterRouteForOnlySysDb<FilesController>(a => a.Delete(string.Empty));
            RegisterRouteForOnlySysDb<FilesController>(a => a.Patch(string.Empty, string.Empty));
            RegisterRouteForOnlySysDb<FilesController>(a => a.Post(string.Empty));
            RegisterRouteForOnlySysDb<FoldersController>(a => a.Subdirectories(null));
            RegisterRouteForOnlySysDb<RdcController>(a => a.Signatures(string.Empty));
            RegisterRouteForOnlySysDb<RdcController>(a => a.Stats());
            RegisterRouteForOnlySysDb<RdcController>(a => a.Manifest(string.Empty));
            RegisterRouteForOnlySysDb<SearchController>(a => a.Terms(string.Empty));
            RegisterRouteForOnlySysDb<SearchController>(a => a.Get(string.Empty, new string[] {}));
            RegisterRouteForOnlySysDb<StaticFSController>(a => a.ClientAccessPolicy());
            RegisterRouteForOnlySysDb<StaticFSController>(a => a.RavenStudioXap());
            RegisterRouteForOnlySysDb<StaticFSController>(a => a.FavIcon());
            RegisterRouteForOnlySysDb<StaticFSController>(a => a.Root());
            RegisterRouteForOnlySysDb<StaticFSController>(a => a.Id());
            RegisterRouteForOnlySysDb<StatsController>(a => a.Get());
            RegisterRouteForOnlySysDb<StorageController>(a => a.RetryRenaming());
            RegisterRouteForOnlySysDb<StorageController>(a => a.CleanUp());
			RegisterRouteForOnlySysDb<SynchronizationController>(a => a.ToDestination(null, false));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.ToDestinations(false));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Start(string.Empty));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.MultipartProceed());
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.UpdateMetadata(string.Empty));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Delete(string.Empty));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Rename(string.Empty, string.Empty));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Confirm());
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Finished());
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Active());
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Pending());
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Conflicts());
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.ResolveConflict(string.Empty, ConflictResolutionStrategy.CurrentVersion));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.ApplyConflict(string.Empty, 0, string.Empty, string.Empty));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.LastSynchronization(Guid.Empty));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.IncrementLastETag(Guid.Empty, string.Empty, Guid.Empty));
            RegisterRouteForOnlySysDb<SynchronizationController>(a => a.Status(null));
			RegisterRouteForOnlySysDb<AdminFileSystemController>(a => a.Put(null));
            RegisterRouteForOnlySysDb<FileSystemsController>(a => a.Stats());
            RegisterRouteForOnlySysDb<FileSystemsController>(a => a.Names());

            RegisterRouteForOnlySysDb<StudioTasksController>(a => a.GetNewEncryption(null));
            RegisterRouteForOnlySysDb<StudioTasksController>(a => a.IsBase64Key(null));

            const string nonSystemDbPrefix = "databases/{databaseName}/";

            var routeMethods = typeof (RavenDbApiController).Assembly
                                                            .DefinedTypes
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
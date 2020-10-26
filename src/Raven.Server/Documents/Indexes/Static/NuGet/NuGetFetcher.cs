using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using NuGet.Common;
using NuGet.Configuration;
using NuGet.Frameworks;
using NuGet.PackageManagement;
using NuGet.Packaging;
using NuGet.Packaging.Core;
using NuGet.Packaging.PackageExtraction;
using NuGet.Packaging.Signing;
using NuGet.ProjectManagement;
using NuGet.Protocol.Core.Types;
using NuGet.Versioning;

namespace Raven.Server.Documents.Indexes.Static.NuGet
{
    public class NuGetFetcher
    {
        private readonly string _rootPath;
        private readonly PackageSource _packageSource;
        private readonly SourceRepository _sourceRepository;
        private readonly SourceCacheContext _sourceCacheContext;
        private readonly FrameworkReducer _frameworkReducer = new FrameworkReducer();
        private readonly NuGetFramework _framework = GetCurrentNuGetFramework();

        private readonly ConcurrentDictionary<(string Package, string Version), Lazy<Task<List<string>>>> _pendingPackages = new ConcurrentDictionary<(string Package, string Version), Lazy<Task<List<string>>>>();

        public NuGetFetcher(string packageSourceUrl, string rootPath)
        {
            _rootPath = rootPath;
            _packageSource = new PackageSource(packageSourceUrl);
            _sourceRepository = new SourceRepository(_packageSource, Repository.Provider.GetCoreV3());
            _sourceCacheContext = new SourceCacheContext();
        }

        public async Task ValidateConnectivity()
        {
            try
            {
                var resource = await _sourceRepository.GetResourceAsync<PackageMetadataResource>();
                if (resource == null)
                    Throw();
            }
            catch (Exception e)
            {
                Throw(e);
            }

            void Throw(Exception e = null)
            {
                throw new InvalidOperationException($"Could not fetch a package metadata resource from '{_sourceRepository.PackageSource.Source}'. Is this a valid NuGet repository?", e);
            }
        }

        public async Task<List<string>> DownloadAsync(string package, string version, CancellationToken token = default)
        {
            var resource = await _sourceRepository.GetResourceAsync<PackageMetadataResource>(token);
            var identity = new PackageIdentity(package, NuGetVersion.Parse(version));
            var metadata = await resource.GetMetadataAsync(identity, _sourceCacheContext, NullLogger.Instance, token);
            if (metadata == null)
                return null;

            var nearest = _frameworkReducer.GetNearest(_framework, metadata.DependencySets.Select(x => x.TargetFramework));
            var downloadTasks = new List<Task<List<string>>>();
            var dependencies = metadata.DependencySets.FirstOrDefault(x => x.TargetFramework == nearest);
            if (dependencies != null)
            {
                foreach (var depPkg in dependencies.Packages)
                {
                    var key = (depPkg.Id, depPkg.VersionRange.MinVersion.ToString());
                    var task = _pendingPackages.GetOrAdd(key, _ => new Lazy<Task<List<string>>>(() => DownloadAsync(depPkg.Id, depPkg.VersionRange.MinVersion.ToString(), token)));
                    downloadTasks.Add(task.Value);
                }
            }

            downloadTasks.Add(DownloadPackageAsync(metadata.Identity, token));
            await Task.WhenAll(downloadTasks);

            return new List<string>(downloadTasks.SelectMany(x => x.Result).Distinct());
        }

        private static NuGetFramework GetCurrentNuGetFramework()
        {
            string frameworkName = Assembly.GetExecutingAssembly().GetCustomAttributes(true)
                .OfType<System.Runtime.Versioning.TargetFrameworkAttribute>()
                .Select(x => x.FrameworkName)
                .First();
            return NuGetFramework.Parse(frameworkName);
        }

        private async Task<List<string>> DownloadPackageAsync(PackageIdentity identity, CancellationToken token)
        {
            var settings = Settings.LoadDefaultSettings(_rootPath);
            var packageSourceProvider = new PackageSourceProvider(settings);
            var sourceRepositoryProvider = new SourceRepositoryProvider(packageSourceProvider, Repository.Provider.GetCoreV3());
            var project = new FolderNuGetProject(_rootPath);
            var packageManager = new NuGetPackageManager(sourceRepositoryProvider, settings, _rootPath)
            {
                PackagesFolderNuGetProject = project
            };

            ResolutionContext resolutionContext = new ResolutionContext();
            var downloadContext = new PackageDownloadContext(resolutionContext.SourceCacheContext,
                _rootPath, resolutionContext.SourceCacheContext.DirectDownload);

            bool packageAlreadyExists = packageManager.PackageExistsInPackagesFolder(identity,
                PackageSaveMode.None);
            if (packageAlreadyExists == false)
            {
                var projectContext = new ProjectContext();
                var clientPolicyContext = ClientPolicyContext.GetClientPolicy(settings, NullLogger.Instance);
                projectContext.PackageExtractionContext = new PackageExtractionContext(PackageSaveMode.Defaultv2, PackageExtractionBehavior.XmlDocFileSaveMode, clientPolicyContext, NullLogger.Instance);

                await packageManager.InstallPackageAsync(
                    project,
                    identity,
                    resolutionContext,
                    projectContext,
                    downloadContext,
                    _sourceRepository,
                    new List<SourceRepository>(),
                    token);
            }

            var archiveReader = new PackageArchiveReader(project.GetInstalledPackageFilePath(identity));
            var frameworkSpecificGroups = (await archiveReader.GetReferenceItemsAsync(token)).ToList();
            var nearest = _frameworkReducer.GetNearest(_framework, frameworkSpecificGroups.Select(x => x.TargetFramework));
            var match = frameworkSpecificGroups.FirstOrDefault(x => x.TargetFramework == nearest);
            if (match == null)
                return new List<string>();

            var list = new List<string>();
            var installedPackagedFolder = project.GetInstalledPath(identity);
            foreach (var item in match.Items)
            {
                if (string.Equals(".dll", Path.GetExtension(item), StringComparison.OrdinalIgnoreCase) == false)
                    continue;
                list.Add(Path.GetFullPath(Path.Combine(installedPackagedFolder, item)));
            }
            return list;
        }
    }
}

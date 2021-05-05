using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
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
using Sparrow.Platform;

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

        private readonly ConcurrentDictionary<(string Package, string Version), Lazy<Task<NuGetPackage>>> _pendingPackages = new ConcurrentDictionary<(string Package, string Version), Lazy<Task<NuGetPackage>>>();

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

        public async Task<NuGetPackage> DownloadAsync(string package, string version, CancellationToken token = default)
        {
            var resource = await _sourceRepository.GetResourceAsync<PackageMetadataResource>(token);
            var identity = new PackageIdentity(package, NuGetVersion.Parse(version));
            var metadata = await resource.GetMetadataAsync(identity, _sourceCacheContext, NullLogger.Instance, token);
            if (metadata == null)
                return null;

            var nearest = _frameworkReducer.GetNearest(_framework, metadata.DependencySets.Select(x => x.TargetFramework));
            var dependencyTasks = new List<Task<NuGetPackage>>();
            var dependencies = metadata.DependencySets.FirstOrDefault(x => x.TargetFramework == nearest);
            if (dependencies != null)
            {
                foreach (var depPkg in dependencies.Packages)
                {
                    var key = (depPkg.Id, depPkg.VersionRange.MinVersion.ToString());
                    var task = _pendingPackages.GetOrAdd(key, _ => new Lazy<Task<NuGetPackage>>(() => DownloadAsync(depPkg.Id, depPkg.VersionRange.MinVersion.ToString(), token)));
                    dependencyTasks.Add(task.Value);
                }
            }

            var packageTask = DownloadPackageAsync(metadata.Identity, token);

            await Task.WhenAll(dependencyTasks);
            await packageTask;

            var pckg = packageTask.Result;
            if (pckg == null)
                return null;

            foreach (var dependencyTask in dependencyTasks)
            {
                var dependency = dependencyTask.Result;
                if (dependency == null)
                    continue;

                pckg.Dependencies.Add(dependency);
            }
                
            return pckg;
        }

        private static NuGetFramework GetCurrentNuGetFramework()
        {
            string frameworkName = Assembly.GetExecutingAssembly().GetCustomAttributes(true)
                .OfType<System.Runtime.Versioning.TargetFrameworkAttribute>()
                .Select(x => x.FrameworkName)
                .First();
            return NuGetFramework.Parse(frameworkName);
        }

        private async Task<NuGetPackage> DownloadPackageAsync(PackageIdentity identity, CancellationToken token)
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
            var hasNative = (await archiveReader.GetSupportedFrameworksAsync(token)).Select(x => x.Framework).Contains("native");

            if (match == null && hasNative == false)
                return null;

            var package = new NuGetPackage
            {
                Path = project.GetInstalledPath(identity)
            };

            if (match != null)
            {
                foreach (var item in match.Items)
                {
                    var itemExtension = Path.GetExtension(item);

                    if (string.Equals(".dll", itemExtension, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    package.Libraries.Add(Path.GetFullPath(Path.Combine(package.Path, item)));
                }
            }

            if (hasNative)
            {
                var runtimeNativeDirectory = GetRuntimeNativeDirectory();
                package.NativePath = Path.GetFullPath(Path.Combine(package.Path, runtimeNativeDirectory));
            }

            return package;
        }

        private static string GetRuntimeNativeDirectory()
        {
            string path = "runtimes";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                path += "/linux";
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                path += "/osx";
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                path += "/win";
            }
            else
            {
                throw new NotSupportedException("TODO ppekrol");
            }

            if (RuntimeInformation.ProcessArchitecture == Architecture.Arm)
                path += "-arm";
            else if (RuntimeInformation.ProcessArchitecture == Architecture.Arm64)
                path += "-arm64";
            else if (PlatformDetails.Is32Bits)
                path += "-x86";
            else
                path += "-x64";

            path += "/native";

            return path;
        }

        public class NuGetPackage
        {
            public string Path { get; set; }

            public string NativePath { get; set; }

            public HashSet<string> Libraries { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            public HashSet<NuGetPackage> Dependencies { get; set; } = new HashSet<NuGetPackage>();

            protected bool Equals(NuGetPackage other)
            {
                return string.Equals(Path, other.Path, StringComparison.OrdinalIgnoreCase)
                       && string.Equals(NativePath, other.NativePath, StringComparison.OrdinalIgnoreCase)
                       && Equals(Libraries, other.Libraries);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((NuGetPackage)obj);
            }

            public override int GetHashCode()
            {
                var hashCode = new HashCode();
                hashCode.Add(Path, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(NativePath, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(Libraries);
                return hashCode.ToHashCode();
            }
        }
    }
}

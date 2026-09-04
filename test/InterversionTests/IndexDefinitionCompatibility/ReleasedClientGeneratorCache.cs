using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace InterversionTests.IndexDefinitionCompatibility;

internal static class ReleasedClientGeneratorCache
{
    private const string GeneratorAssemblyName = "ReleasedRavenClientDefinitionGenerator";
    private const string GeneratedProgramFileName = "Program.cs";
    private const string TargetFramework = "net8.0";
    private const string RuntimeFrameworkVersion = "8.0.29";
    private const string Configuration = "Release";

    private static readonly string[] GeneratorSourceRelativePaths =
    [
        Path.Combine("DefinitionCases", "DefinitionCase.cs"),
        "DefinitionGeneratorProgram.cs",
        "IndexDefinitionCompatibilityContract.cs",
        Path.Combine("DefinitionCases", "StructuralDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "GeneratedDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "MemoryExtensionsDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "BooleanArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "CharArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "DateTimeArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "DecimalArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "DoubleArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "FloatArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "Int32ArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "Int64ArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "StringArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "UInt64ArrayDefinitionCases.cs"),
        Path.Combine("DefinitionCases", "DefinitionCaseDocuments.cs")
    ];

    public static async Task<CachedGenerator> GetAsync(string clientVersion, string compilerFingerprint)
    {
        var repositoryRoot = FindRepositoryRoot();
        var expectedSdkVersion = GetCompilerFingerprintPart(compilerFingerprint, "sdk");
        var sdkVersion = await RunDotNet(repositoryRoot, "--version");
        if (sdkVersion.ExitCode != 0)
            throw new InvalidOperationException(BuildProcessFailure("Unable to resolve the dotnet SDK.", repositoryRoot, sdkVersion));

        var actualSdkVersion = sdkVersion.StandardOutput.Trim();
        if (string.Equals(expectedSdkVersion, actualSdkVersion, StringComparison.Ordinal) == false)
            throw new InvalidOperationException($"InterversionTests was compiled with .NET SDK '{expectedSdkVersion}', but runtime 'dotnet' resolves to '{actualSdkVersion}'. " +
                                                "The released client generator must use the same SDK as the test assembly.");

        var sourceDirectory = Path.Combine(repositoryRoot, "test", "InterversionTests", "IndexDefinitionCompatibility");
        var sourcePaths = GeneratorSourceRelativePaths
            .Select(fileName => Path.Combine(sourceDirectory, fileName))
            .ToArray();
        foreach (string sourcePath in sourcePaths)
        {
            if (File.Exists(sourcePath) == false)
                throw new FileNotFoundException("A released client generator source file was not found.", sourcePath);
        }

        var nugetConfigPath = Path.Combine(repositoryRoot, "NuGet.Config");
        if (File.Exists(nugetConfigPath) == false)
            throw new FileNotFoundException("The repository NuGet.Config was not found.", nugetConfigPath);

        var generatedProgram = CreateProgramFile();
        var canonicalProject = CreateProjectFile(
            clientVersion,
            "<repository>/test/InterversionTests/IndexDefinitionCompatibility",
            "<repository>/NuGet.Config");
        var cacheKey = CalculateCacheKey(clientVersion, compilerFingerprint, canonicalProject, generatedProgram, sourcePaths);
        var cacheRoot =
            Environment.GetEnvironmentVariable("RAVEN_INTERVERSIONTEST_CLIENT_DIR") ??
            Path.Combine(Path.GetTempPath(), "RavenClientsForTesting");
        var cacheDirectory = Path.Combine(cacheRoot, clientVersion, cacheKey.Substring(0, 24));
        var projectPath = Path.Combine(cacheDirectory, GeneratorAssemblyName + ".csproj");
        var programPath = Path.Combine(cacheDirectory, GeneratedProgramFileName);
        var assemblyPath = Path.Combine(cacheDirectory, "bin", Configuration, TargetFramework, GeneratorAssemblyName + ".dll");
        var completionPath = Path.Combine(cacheDirectory, "build-complete.txt");

        Directory.CreateDirectory(cacheDirectory);
        if (IsComplete(completionPath, assemblyPath, cacheKey))
            return new CachedGenerator(assemblyPath, cacheDirectory, wasBuilt: false);

        var lockPath = Path.Combine(cacheDirectory, "build.lock");
        await using (await AcquireBuildLock(lockPath))
        {
            if (IsComplete(completionPath, assemblyPath, cacheKey))
                return new CachedGenerator(assemblyPath, cacheDirectory, wasBuilt: false);

            var project = CreateProjectFile(clientVersion, sourceDirectory, nugetConfigPath);
            await File.WriteAllTextAsync(projectPath, project, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            await File.WriteAllTextAsync(programPath, generatedProgram, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

            var build = await RunDotNet(
                repositoryRoot,
                "build",
                projectPath,
                "--configuration",
                Configuration,
                "--nologo",
                "--verbosity:minimal");
            await File.WriteAllTextAsync(Path.Combine(cacheDirectory, "build.stdout.log"), build.StandardOutput);
            await File.WriteAllTextAsync(Path.Combine(cacheDirectory, "build.stderr.log"), build.StandardError);

            if (build.ExitCode != 0)
            {
                throw new InvalidOperationException(BuildProcessFailure(
                    $"Building the released RavenDB.Client {clientVersion} definition generator failed. Cache directory: '{cacheDirectory}'.",
                    repositoryRoot,
                    build));
            }

            if (File.Exists(assemblyPath) == false)
                throw new FileNotFoundException("The released client generator build did not produce its assembly.", assemblyPath);

            var temporaryCompletionPath = completionPath + ".tmp";
            await File.WriteAllTextAsync(temporaryCompletionPath, cacheKey, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            File.Move(temporaryCompletionPath, completionPath, overwrite: true);
        }

        return new CachedGenerator(assemblyPath, cacheDirectory, wasBuilt: true);
    }

    private static bool IsComplete(string completionPath, string assemblyPath, string cacheKey)
    {
        if (File.Exists(completionPath) == false || File.Exists(assemblyPath) == false)
            return false;

        try
        {
            return string.Equals(File.ReadAllText(completionPath), cacheKey, StringComparison.Ordinal);
        }
        catch (IOException)
        {
            return false;
        }
    }

    private static async Task<FileStream> AcquireBuildLock(string lockPath)
    {
        var timeout = Stopwatch.StartNew();
        while (timeout.Elapsed < TimeSpan.FromMinutes(10))
        {
            try
            {
                return new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                await Task.Delay(250);
            }
        }

        throw new TimeoutException($"Timed out waiting for the released client generator cache lock '{lockPath}'.");
    }

    private static string CalculateCacheKey(
        string clientVersion,
        string compilerFingerprint,
        string canonicalProject,
        string generatedProgram,
        IReadOnlyList<string> sourcePaths)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendText(hash, clientVersion);
        AppendText(hash, compilerFingerprint);
        AppendText(hash, canonicalProject);
        AppendText(hash, GeneratedProgramFileName);
        AppendText(hash, generatedProgram);

        foreach (string sourcePath in sourcePaths.OrderBy(x => x, StringComparer.Ordinal))
        {
            AppendText(hash, Path.GetFileName(sourcePath));
            hash.AppendData(File.ReadAllBytes(sourcePath));
        }

        return Convert.ToHexString(hash.GetHashAndReset());
    }

    private static void AppendText(IncrementalHash hash, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        hash.AppendData(BitConverter.GetBytes(bytes.Length));
        hash.AppendData(bytes);
    }

    private static string CreateProjectFile(string clientVersion, string sourceDirectory, string nugetConfigPath)
    {
        var compileItems = string.Join(
            Environment.NewLine,
            GeneratorSourceRelativePaths.Select(fileName =>
                $"    <Compile Include=\"{EscapeXml(Path.Combine(sourceDirectory, fileName))}\" Link=\"{fileName}\" />")) +
            Environment.NewLine +
            $"    <Compile Include=\"{GeneratedProgramFileName}\" />";

        // language=XML
        return $"""
               <Project Sdk="Microsoft.NET.Sdk">
                 <PropertyGroup>
                   <OutputType>Exe</OutputType>
                   <TargetFramework>{TargetFramework}</TargetFramework>
                   <RuntimeFrameworkVersion>{RuntimeFrameworkVersion}</RuntimeFrameworkVersion>
                   <LangVersion>Preview</LangVersion>
                   <Nullable>disable</Nullable>
                   <ImplicitUsings>disable</ImplicitUsings>
                   <EnableDefaultCompileItems>false</EnableDefaultCompileItems>
                   <ManagePackageVersionsCentrally>false</ManagePackageVersionsCentrally>
                   <Deterministic>true</Deterministic>
                   <ContinuousIntegrationBuild>true</ContinuousIntegrationBuild>
                   <AssemblyName>{GeneratorAssemblyName}</AssemblyName>
                   <RestoreConfigFile>{EscapeXml(nugetConfigPath)}</RestoreConfigFile>
                 </PropertyGroup>
                 <ItemGroup>
               {compileItems}
                 </ItemGroup>
                 <ItemGroup>
                   <PackageReference Include="RavenDB.Client" Version="{EscapeXml(clientVersion)}" />
                 </ItemGroup>
                 <ItemGroup>
                   <AssemblyAttribute Include="System.Reflection.AssemblyMetadataAttribute">
                     <_Parameter1>CompilerFingerprint</_Parameter1>
                     <_Parameter2>sdk=$(NETCoreSdkVersion);msbuild=$(MSBuildVersion);lang=$(LangVersion);tfm=$(TargetFramework)</_Parameter2>
                   </AssemblyAttribute>
                 </ItemGroup>
               </Project>
               """;
    }

    private static string CreateProgramFile()
    {
        // language=csharp
        return """
               using System;
               using System.Text.Json;
               using InterversionTests.IndexDefinitionCompatibility;

               internal static class Program
               {
                   public static int Main()
                   {
                       try
                       {
                           Console.Write(JsonSerializer.Serialize(DefinitionGeneratorProgram.Generate()));
                           return 0;
                       }
                       catch (Exception e)
                       {
                           Console.Error.WriteLine(e);
                           return 1;
                       }
                   }
               }
               """;
    }

    private static string EscapeXml(string value)
    {
        return SecurityElement.Escape(value);
    }

    private static string GetCompilerFingerprintPart(string compilerFingerprint, string name)
    {
        var prefix = name + "=";
        var part = compilerFingerprint.Split(';')
            .SingleOrDefault(x => x.StartsWith(prefix, StringComparison.Ordinal));

        return part != null
            ? part.Substring(prefix.Length)
            : throw new InvalidOperationException($"Compiler fingerprint '{compilerFingerprint}' has no '{name}' component.");
    }

    private static string FindRepositoryRoot()
    {
        var startingDirectories = new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory };
        foreach (string startingDirectory in startingDirectories.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var directory = new DirectoryInfo(startingDirectory);
            while (directory != null)
            {
                if (File.Exists(Path.Combine(directory.FullName, "RavenDB.sln")))
                    return directory.FullName;
                directory = directory.Parent;
            }
        }

        throw new DirectoryNotFoundException(
            "Could not find the RavenDB repository root from: " + string.Join(", ", startingDirectories));
    }

    private static async Task<ProcessResult> RunDotNet(string workingDirectory, params string[] arguments)
    {
         var startInfo = new ProcessStartInfo
        {
            FileName = "dotnet",
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        foreach (string argument in arguments)
            startInfo.ArgumentList.Add(argument);

        using var process = Process.Start(startInfo);
        if (process == null)
            throw new InvalidOperationException("Could not start dotnet.");

        var standardOutput = process.StandardOutput.ReadToEndAsync();
        var standardError = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        return new ProcessResult(
            process.ExitCode,
            await standardOutput,
            await standardError,
            command: $"dotnet {string.Join(" ", arguments.Select(QuoteArgument))}");
    }

    private static string QuoteArgument(string argument)
    {
        return argument.Any(char.IsWhiteSpace) ? $"\"{argument}\"" : argument;
    }

    private static string BuildProcessFailure(string message, string workingDirectory, ProcessResult result) =>
        $"{message}{Environment.NewLine}" +
        $"Working directory: {workingDirectory}{Environment.NewLine}" +
        $"Command: {result.Command}{Environment.NewLine}" +
        $"Exit code: {result.ExitCode}{Environment.NewLine}" +
        $"STDOUT:{Environment.NewLine}{result.StandardOutput}{Environment.NewLine}" +
        $"STDERR:{Environment.NewLine}{result.StandardError}";

    internal sealed class CachedGenerator
    {
        public CachedGenerator(string assemblyPath, string cacheDirectory, bool wasBuilt)
        {
            AssemblyPath = assemblyPath;
            CacheDirectory = cacheDirectory;
            WasBuilt = wasBuilt;
        }

        public string AssemblyPath { get; }
        public string CacheDirectory { get; }
        public bool WasBuilt { get; }
    }

    private sealed class ProcessResult
    {
        public ProcessResult(int exitCode, string standardOutput, string standardError, string command)
        {
            ExitCode = exitCode;
            StandardOutput = standardOutput;
            StandardError = standardError;
            Command = command;
        }

        public int ExitCode { get; }
        public string StandardOutput { get; }
        public string StandardError { get; }
        public string Command { get; }
    }
}

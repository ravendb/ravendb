using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Sparrow.Platform;

namespace Tests.Infrastructure.InterversionTest
{
    public class ServerBuildRetriever
    {
        private const string S3BucketName = "daily-builds";

        private static readonly string _downloadsS3BucketUrl = $"https://{S3BucketName}.s3.amazonaws.com";

        private readonly SemaphoreSlim _downloadQueueSemaphore = new SemaphoreSlim(1, 1);

        private HttpClient _httpClient;

        private HttpClient DownloadClient
        {
            get
            {
                if (_httpClient == null)
                {
                    _httpClient = new HttpClient
                    {
                        Timeout = TimeSpan.FromSeconds(600)
                    };
                }

                return _httpClient;
            }
        }

        private string _serverDownloadPath;

        private string ServerDownloadPath
        {
            get
            {
                if (string.IsNullOrEmpty(_serverDownloadPath) == false)
                {
                    return _serverDownloadPath;
                }

                var path = Environment.GetEnvironmentVariable("RAVEN_INTERVERSIONTEST_SERVER_DIR") ?? Path.Combine(Path.GetTempPath(), "RavenServersForTesting");

                if (Directory.Exists(path) == false)
                {
                    Directory.CreateDirectory(path);
                }

                _serverDownloadPath = path;

                return _serverDownloadPath;
            }
        }

        public async Task<string> GetServerPath(
            ServerBuildDownloadInfo serverInfo, CancellationToken token = default)
        {
            await _downloadQueueSemaphore.WaitAsync(token);
            var serverDirectory = serverInfo.GetServerDirectory(ServerDownloadPath);
            var packagePath = Path.Combine(ServerDownloadPath, serverInfo.PackageFileName);
            try
            {
                if (Directory.Exists(serverDirectory))
                    return serverDirectory;

                if (File.Exists(packagePath) == false)
                {
                    await DownloadServerPackage(serverInfo, token);
                }

                await UnpackServerPackage(packagePath, serverDirectory);

                return serverDirectory;
            }
            catch (Exception)
            {
                if (File.Exists(packagePath))
                    IOExtensions.DeleteFile(packagePath);

                if (Directory.Exists(serverDirectory))
                    IOExtensions.DeleteDirectory(serverDirectory);

                throw;
            }
            finally
            {
                _downloadQueueSemaphore.Release();
            }
        }

        private static async Task UnpackServerPackage(string packagePath, string targetDirectory)
        {
            if (PlatformDetails.RunningOnPosix)
            {
                Directory.CreateDirectory(targetDirectory);
                var extractTarBall = Process.Start(
                    "tar", $"xjf \"{packagePath}\" -C \"{targetDirectory}\" --strip-components=1");
                extractTarBall.WaitForExit();

                if (extractTarBall.ExitCode != 0)
                {
                    var unpackOutput = await extractTarBall.StandardOutput.ReadToEndAsync();
                    throw new InvalidOperationException(
                        $"Unpacking {packagePath} failed (exit code {extractTarBall.ExitCode}): {unpackOutput}");
                }
            }
            else
            {
                ZipFile.ExtractToDirectory(packagePath, targetDirectory);
            }

        }

        private async Task<string> DownloadServerPackage(
            ServerBuildDownloadInfo serverInfo, CancellationToken token = default)
        {
            var pkgDownloadUrl = $"{_downloadsS3BucketUrl}/{serverInfo.PackageFileName}";
            var downloadFilePath = Path.Combine(ServerDownloadPath, serverInfo.PackageDownloadFileName);
            var packageFilePath = Path.Combine(ServerDownloadPath, serverInfo.PackageFileName);
            try
            {
                var response = await DownloadClient.GetAsync(pkgDownloadUrl, HttpCompletionOption.ResponseHeadersRead, token);
                if (response.IsSuccessStatusCode == false)
                {
                    var msg = await response.Content.ReadAsStringAsync();
                    throw new InvalidOperationException(
                        $"Error downloading {pkgDownloadUrl}: {response.StatusCode} {msg}");
                }

                using (var stream = await response.Content.ReadAsStreamAsync())
                {
                    using (var file = File.OpenWrite(downloadFilePath))
                    {
                        await stream.CopyToAsync(file, token);
                    }
                }

                File.Move(downloadFilePath, packageFilePath);

                return packageFilePath;
            }
            catch (Exception)
            {
                if (File.Exists(downloadFilePath))
                    IOExtensions.DeleteFile(downloadFilePath);

                if (File.Exists(packageFilePath))
                    IOExtensions.DeleteFile(packageFilePath);

                throw;
            }
        }
    }
}

// -----------------------------------------------------------------------
//  <copyright file="RavenFtpClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using FluentFTP;
using FluentFTP.Client.BaseClient;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup
{
    public sealed class RavenFtpClient : RavenStorageClient
    {
        private readonly string _url;
        private readonly int? _port;
        private readonly string _userName;
        private readonly string _password;
        private readonly string _certificateAsBase64;
        private const int DefaultFtpPort = 21;
        public static bool ValidateAnyCertificate = false;

        public RavenFtpClient(FtpSettings ftpSettings, Progress progress = null, CancellationToken? cancellationToken = null)
            : base(progress, cancellationToken)
        {
            _url = ftpSettings.Url;
            _userName = ftpSettings.UserName;
            _password = ftpSettings.Password;
            _certificateAsBase64 = ftpSettings.CertificateAsBase64;

            if (_url.StartsWith("ftp://", StringComparison.OrdinalIgnoreCase) == false &&
                _url.StartsWith("ftps://", StringComparison.OrdinalIgnoreCase) == false)
                _url = "ftp://" + _url;

            if (_url.StartsWith("ftps://", StringComparison.OrdinalIgnoreCase))
                _url = _url.Replace("ftps://", "ftp://", StringComparison.OrdinalIgnoreCase);

            if (_url.EndsWith("/") == false)
                _url += "/";

            var uri = new Uri(_url);
            if (uri.IsDefaultPort == false)
                _port = uri.Port;
        }

        /// <summary>
        /// Uploading a give file stream to a remote ftp server
        /// </summary>
        /// <param name="folderName">The name of the folder that will be created for the file</param>
        /// <param name="fileName">The name of the file that will be created in the remote ftp server</param>
        /// <param name="stream">The requested file stream to upload</param>
        public void UploadFile(string folderName, string fileName, Stream stream)
        {
            TestConnection();

            Progress?.UploadProgress.SetTotal(stream.Length);
            Progress?.UploadProgress.ChangeState(UploadState.PendingUpload);

            var url = CreateNestedFoldersIfNeeded(folderName, out string path);
            path += $"/{fileName}";

            using (var client = CreateFtpClient(url, keepAlive: true))
            {
                try
                {
                    client.UploadStream(stream, path, progress: (p) =>
                    {
                        if (Progress == null)
                            return;
                        switch (p.Progress)
                        {
                            case 100:
                                Progress?.UploadProgress.ChangeState(UploadState.PendingResponse);
                                break;
                            case 0:
                                Progress?.UploadProgress.ChangeState(UploadState.PendingUpload);
                                break;
                            case > 0 and < 100:
                                Progress?.UploadProgress.ChangeState(UploadState.Uploading);
                                break;
                        }
                        Progress?.UploadProgress.SetUploaded(p.TransferredBytes);
                        Progress?.OnUploadProgress();
                    });
                }
                finally
                {
                    Progress?.UploadProgress.ChangeState(UploadState.PendingResponse);
                    if (client.FileExists(path + "/" + fileName))
                    {
                        Progress?.UploadProgress.ChangeState(UploadState.Done);
                    }
                }
            }
        }

        /// <summary>
        /// Creating the folder, include subfolders, for the uploaded file as specified in the url
        /// </summary>
        /// <param name="folderName">The folder name the uploaded file will upload into</param>
        /// <param name="path">The extracted path including all the subfolders and the created folder</param>
        /// <returns>The url without any subfolders</returns>
        private string CreateNestedFoldersIfNeeded(string folderName, out string path)
        {
            ExtractUrlAndDirectories(out var url, out var directories);

            // create the nested folders including the new folder
            directories.Add(folderName);
            path = string.Empty;
            foreach (var directory in directories)
            {
                if (directory == string.Empty)
                    continue;

                path += $"/{directory}";
                using (var client = CreateFtpClient(url, keepAlive: false))
                {
                    if (!client.DirectoryExists(path))
                        client.CreateDirectory(path);
                }
            }

            return url;
        }

        /// <summary>
        /// Generating the url without any subfolders and a list of those subfolders given the original url
        /// </summary>
        /// <param name="url">The url without any subfolders from the original url</param>
        /// <param name="dirs">The list of subfolders from the original url</param>
        private void ExtractUrlAndDirectories(out string url, out List<string> dirs)
        {
            var uri = new Uri(_url);

            dirs = uri.AbsolutePath.TrimStart('/').TrimEnd('/').Split("/").ToList();

            url = $"{uri.Scheme}://{uri.Host}";
        }

        /// <summary>
        /// Creating an instance of ftp client
        /// </summary>
        /// <param name="url">The ftp server url, include subfolder/s in case of needed</param>
        /// <param name="keepAlive">The condition that control if the ftp client whether should be keep alive or not</param>
        /// <returns>The ftp client instance</returns>
        /// <exception cref="ArgumentException">In case of invalid certificate</exception>
        internal FtpClient CreateFtpClient(string url, bool keepAlive)
        {
            var client = new FtpClient(url);
            client.Config.ConnectTimeout = 0;
            client.Credentials = new NetworkCredential(_userName, _password);
            if (string.IsNullOrEmpty(_certificateAsBase64) == false)
            {
                var byteArray = Convert.FromBase64String(_certificateAsBase64);

                try
                {
                    var x509Certificate = X509CertificateLoader.LoadCertificate(byteArray);
                    client.Config.EncryptionMode = FtpEncryptionMode.Explicit;
                    client.Config.ClientCertificates.Add(x509Certificate);
                    client.ValidateCertificate += OnValidateCertificate;
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Could not load certificate.", e);
                }
            }

            client.Config.SocketKeepAlive = keepAlive;
            if (_port != null)
                client.Port = (int)_port;
            else
                client.Port = DefaultFtpPort;
            client.Connect();
            return client;
        }

        private static void OnValidateCertificate(BaseFtpClient control, FtpSslValidationEventArgs e)
        {
            if (e.PolicyErrors == SslPolicyErrors.None || ValidateAnyCertificate)
            {
                e.Accept = true;
            }
            else
            {
                throw new AuthenticationException($"Couldn't establish connection using this certificate due to these errors: {e.PolicyErrors}");
            }
        }

        /// <summary>
        /// Testing the connection between the ftp client and the ftp server
        /// </summary>
        /// <exception cref="ArgumentException">When giving invalid certificate</exception>
        /// <exception cref="AuthenticationException">When giving wrong certificate or url not matched</exception>
        public void TestConnection()
        {
            ExtractUrlAndDirectories(out var url, out var _);
            using (var client = CreateFtpClient(url, keepAlive: false))
            {
                try
                {
                    client.Connect();
                }
                catch (AuthenticationException e)
                {
                    throw new AuthenticationException("Make sure that you provided the correct certificate " +
                                                      "and that the url matches the one that is defined in it", e);
                }
            }
        }

        /// <summary>
        /// Getting the url directories path
        /// </summary>
        /// <returns>A string of all the directories</returns>
        private string GetPath()
        {
            var path = "";
            ExtractUrlAndDirectories(out var _, out var dirs);
            foreach (string dir in dirs)
            {
                path += "/" + dir;
            }

            return path;
        }

        /// <summary>
        /// Getting a list of items according to which object type is passed
        /// </summary>
        /// <param name="url">The ftp server url</param>
        /// <param name="path">The path to get from</param>
        /// <param name="type">The object type to filter</param>
        /// <returns>A complete list of the items after filtered</returns>
        private List<string> GetItemsInternal(string url, string path, FtpObjectType type)
        {
            var list = new List<string>();
            using (var client = CreateFtpClient(url, keepAlive: false))
            {
                foreach (FtpListItem ftpListItem in client.GetListing(path))
                {
                    if (ftpListItem.Type == type)
                    {
                        list.Add(ftpListItem.FullName);
                    }
                }
            }

            return list;
        }

        /// <summary>
        /// Getting all the folders that are in the ftp server according the original url
        /// </summary>
        /// <returns>A complete list of all the folders names</returns>
        public List<string> GetFolders()
        {
            TestConnection();
            ExtractUrlAndDirectories(out var url, out _);
            var path = GetPath();
            return GetItemsInternal(url, path, FtpObjectType.Directory);
        }

        /// <summary>
        /// Getting all the files in a specific folder name
        /// </summary>
        /// <param name="folderName">The folder name to look for all the files inside it</param>
        /// <returns>A complete list of all files names</returns>
        /// <exception cref="ArgumentException">When giving invalid certificate</exception>
        public List<string> GetFiles([NotNull] string folderName)
        {
            if (string.IsNullOrEmpty(folderName))
                throw new ArgumentException("Value cannot be null or empty.", nameof(folderName));
            ExtractUrlAndDirectories(out var url, out _);
            return GetItemsInternal(url, folderName, FtpObjectType.File);
        }

        /// <summary>
        /// Deleting a specific folder in the ftp server
        /// </summary>
        /// <param name="folderName">The folder to which to delete in the ftp server</param>
        /// <exception cref="ArgumentException">When giving invalid certificate</exception>
        public void DeleteFolder([NotNull] string folderName)
        {
            if (string.IsNullOrEmpty(folderName))
                throw new ArgumentException("Value cannot be null or empty.", nameof(folderName));
            ExtractUrlAndDirectories(out var url, out _);
            using (var client = CreateFtpClient(url, keepAlive: false))
            {
                client.DeleteDirectory(folderName);
            }
        }
    }
}

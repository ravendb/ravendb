// -----------------------------------------------------------------------
//  <copyright file="RavenFtpClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using FluentFTP;
using FluentFTP.Client.BaseClient;
using FluentFTP.Exceptions;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RavenFtpClient : RavenStorageClient
    {
        private readonly string _url;
        private readonly int? _port;
        private readonly string _userName;
        private readonly string _password;
        private readonly string _certificateAsBase64;
        private readonly string _certificateFileName;
        private readonly bool _useSsl;
        private const int DefaultBufferSize = 81920;
        private const int DefaultFtpPort = 21;
        private readonly bool _isTesting;

        public RavenFtpClient(FtpSettings ftpSettings, Progress progress = null, CancellationToken? cancellationToken = null)
            : base(progress, cancellationToken)
        {
            _url = ftpSettings.Url;
            _port = ftpSettings.Port;
            _userName = ftpSettings.UserName;
            _password = ftpSettings.Password;
            _certificateAsBase64 = ftpSettings.CertificateAsBase64;
            _certificateFileName = ftpSettings.CertificateFileName;

            if (_url.StartsWith("ftp://", StringComparison.OrdinalIgnoreCase) == false &&
                _url.StartsWith("ftps://", StringComparison.OrdinalIgnoreCase) == false)
                _url = "ftp://" + _url;

            if (_url.StartsWith("ftps", StringComparison.OrdinalIgnoreCase))
            {
                _useSsl = true;
                _url = _url.Replace("ftps://", "ftp://", StringComparison.OrdinalIgnoreCase);
            }

            if (_url.EndsWith("/") == false)
                _url += "/";

            Debug.Assert(_url.StartsWith("ftp://", StringComparison.OrdinalIgnoreCase));
            _isTesting = Environment.GetEnvironmentVariable("isTesting") != null;
        }

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
                    var readBuffer = new byte[DefaultBufferSize];

                    int count;
                    while ((count = stream.Read(readBuffer, 0, readBuffer.Length)) != 0)
                    {
                        client.UploadBytes(readBuffer, path, FtpRemoteExists.Resume, progress: (p) =>
                        {
                            if (Progress == null)
                                return;
                            switch (p.Progress)
                            {
                                case 100:
                                    Progress?.UploadProgress.ChangeState(UploadState.Done);
                                    break;
                                case 0:
                                    Progress?.UploadProgress.ChangeState(UploadState.PendingUpload);
                                    break;
                                case > 0 and < 100:
                                    Progress?.UploadProgress.ChangeState(UploadState.Uploading);
                                    break;
                            }

                        });
                        Progress?.UploadProgress.UpdateUploaded(count);
                        Progress?.OnUploadProgress();
                    }
                }
                finally
                {
                    Progress?.UploadProgress.ChangeState(UploadState.PendingResponse);
                    if (client.FileExists(url))
                    {
                        Progress?.UploadProgress.ChangeState(UploadState.Done);
                    }
                }
            }
        }

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

        private void ExtractUrlAndDirectories(out string url, out List<string> dirs)
        {
            var uri = new Uri(_url);

            dirs = uri.AbsolutePath.TrimStart('/').TrimEnd('/').Split("/").ToList();

            url = $"{uri.Scheme}://{uri.Host}";
        }

        internal FtpClient CreateFtpClient(string url, bool keepAlive)
        {
            var client = new FtpClient(url);
            client.Config.ConnectTimeout = -1;
            client.Credentials = new NetworkCredential(_userName, _password);
            if (_useSsl)
            {
                var byteArray = Convert.FromBase64String(_certificateAsBase64);

                try
                {
                    var x509Certificate = new X509Certificate2(byteArray);
                    client.Config.EncryptionMode = FtpEncryptionMode.Explicit;
                    client.Config.ClientCertificates.Add(x509Certificate);
                    if (_isTesting)
                        client.Config.ValidateAnyCertificate = true;
                    else
                        client.ValidateCertificate += new FtpSslValidation(OnValidateCertificate);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"This is not a valid certificate, file name: {_certificateFileName}", e);
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
            if (e.PolicyErrors == SslPolicyErrors.None)
            {
                e.Accept = true;
            }
            else
            {
                throw new Exception($"{e.PolicyErrors}{Environment.NewLine}{e.Certificate}");
            }
        }

        public void TestConnection()
        {
            if (_useSsl && string.IsNullOrWhiteSpace(_certificateAsBase64))
                throw new ArgumentException("Certificate must be provided when using ftp with SSL!");
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

        public List<string> GetFolders()
        {
            TestConnection();
            ExtractUrlAndDirectories(out var url, out _);
            var path = GetPath();
            return GetItemsInternal(url, path, FtpObjectType.Directory);
        }

        public List<string> GetFiles([NotNull] string folderName)
        {
            if (string.IsNullOrEmpty(folderName)) throw new ArgumentException("Value cannot be null or empty.", nameof(folderName));
            ExtractUrlAndDirectories(out var url, out _);
            return GetItemsInternal(url, folderName, FtpObjectType.File);
        }

        public void DeleteFolder([NotNull] string folderName)
        {
            if (string.IsNullOrEmpty(folderName)) throw new ArgumentException("Value cannot be null or empty.", nameof(folderName));
            ExtractUrlAndDirectories(out var url, out _);
            using (var client = CreateFtpClient(url, keepAlive: false))
            {
                client.DeleteDirectory(folderName);
            }
        }
    }
}

// -----------------------------------------------------------------------
//  <copyright file="RavenFtpClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
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
        }

        public void UploadFile(string folderName, string fileName, Stream stream)
        {
            TestConnection();

            Progress?.UploadProgress.SetTotal(stream.Length);
            Progress?.UploadProgress.ChangeState(UploadState.PendingUpload);

            var url = CreateNestedFoldersIfNeeded(folderName);
            url += $"/{fileName}";

            using (var client = CreateFtpClient(_url, keepAlive: true))
            {
                var readBuffer = new byte[DefaultBufferSize];

                int count;
                while ((count = stream.Read(readBuffer, 0, readBuffer.Length)) != 0)
                {
                    client.UploadBytes(readBuffer, url, FtpRemoteExists.Resume);

                    Progress?.UploadProgress.ChangeState(UploadState.Uploading);
                    Progress?.UploadProgress.UpdateUploaded(count);
                    Progress?.OnUploadProgress();
                }

                Progress?.UploadProgress.ChangeState(UploadState.PendingResponse);
                if (client.FileExists(url))
                {
                    Progress?.UploadProgress.ChangeState(UploadState.Done);
                }
            }
        }

        private string CreateNestedFoldersIfNeeded(string folderName)
        {
            ExtractUrlAndDirectories(out var directories);

            // create the nested folders including the new folder
            directories.Add(folderName);
            string url = "";

            foreach (var directory in directories)
            {
                if (directory == string.Empty)
                    continue;

                url += $"/{directory}";
                using (var client = CreateFtpClient(_url, keepAlive: false))
                {
                    try
                    {
                        if (!client.DirectoryExists($"/{directory}"))
                            client.CreateDirectory($"/{directory}");
                    }
                    catch (WebException e)
                    {
                        throw ;
                    }
                }
            }

            return url;
        }

        private void ExtractUrlAndDirectories(out List<string> dirs)
        {
            var uri = new Uri(_url);

            dirs = uri.AbsolutePath.TrimStart('/').TrimEnd('/').Split("/").ToList();
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
                    client.Config.SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13;
                    client.Config.ClientCertificates.Add(x509Certificate);
                    client.ValidateCertificate += (control, e) =>
                    {
                        e.Accept = e.PolicyErrors == SslPolicyErrors.None;
                    };
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

        public void TestConnection()
        {
            if (_useSsl && string.IsNullOrWhiteSpace(_certificateAsBase64))
                throw new ArgumentException("Certificate must be provided when using ftp with SSL!");

            using (var client = CreateFtpClient(_url, keepAlive: false))
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
                catch (WebException e)
                {
                    throw;
                }
            }
        }
    }
}

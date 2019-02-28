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
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
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

        public RavenFtpClient(string url, int? port, string userName, string password, string certificateAsBase64,
            string certificateFileName, Progress progress = null, CancellationToken? cancellationToken = null)
            : base(progress, cancellationToken)
        {
            _url = url;
            _port = port;
            _userName = userName;
            _password = password;
            _certificateAsBase64 = certificateAsBase64;
            _certificateFileName = certificateFileName;

            if (_url.StartsWith("ftp://", StringComparison.OrdinalIgnoreCase) == false &&
                _url.StartsWith("ftps://", StringComparison.OrdinalIgnoreCase) == false)
                _url = "ftp://" + url;

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

            Stream requestStream = null;
            using (CancellationToken.Register(() =>
            {
                stream?.Dispose();
                requestStream?.Dispose();
            }))
            {
                Progress?.UploadProgress.SetTotal(stream.Length);
                Progress?.UploadProgress.ChangeState(UploadState.PendingUpload);

                var url = CreateNestedFoldersIfNeeded(folderName);
                url += $"/{fileName}";

                var request = CreateFtpWebRequest(url, WebRequestMethods.Ftp.UploadFile, keepAlive: true);
                var readBuffer = new byte[DefaultBufferSize];

                using (requestStream = request.GetRequestStream())
                {
                    int count;
                    while ((count = stream.Read(readBuffer, 0, readBuffer.Length)) != 0)
                    {
                        requestStream.Write(readBuffer, 0, count);

                        Progress?.UploadProgress.ChangeState(UploadState.Uploading);
                        Progress?.UploadProgress.UpdateUploaded(count);
                        Progress?.OnUploadProgress();
                    }

                    requestStream.Flush();
                }

                Progress?.UploadProgress.ChangeState(UploadState.PendingResponse);
                using (request.GetResponse())
                {
                    Progress?.UploadProgress.ChangeState(UploadState.Done);
                }
            }
        }

        private string CreateNestedFoldersIfNeeded(string folderName)
        {
            ExtractUrlAndDirectories(out var url, out var directories);

            // create the nested folders including the new folder
            directories.Add(folderName);

            foreach (var directory in directories)
            {
                if (directory == string.Empty)
                    continue;

                url += $"/{directory}";
                var request = CreateFtpWebRequest(url, WebRequestMethods.Ftp.MakeDirectory, keepAlive: false);

                try
                {
                    var response = request.GetResponse();
                    response.Close();
                }
                catch (WebException e)
                {
                    var response = (FtpWebResponse)e.Response;
                    if (response.StatusCode == FtpStatusCode.ActionNotTakenFileUnavailable)
                    {
                        // folder already exists
                        continue;
                    }

                    throw;
                }
            }

            return url;
        }

        private void ExtractUrlAndDirectories(out string url, out List<string> dirs)
        {
            var uri = new Uri(_url);

            dirs = uri.AbsolutePath.TrimStart('/').TrimEnd('/').Split("/").ToList();
            var port = _port ?? (uri.Port > 0 ? uri.Port : DefaultFtpPort);
            if (port < 1 || port > 65535)
                throw new ArgumentException("Port number range: 1-65535");

            url = $"{uri.Scheme}://{uri.Host}:{port}";
        }

        private FtpWebRequest CreateFtpWebRequest(string url, string method, bool keepAlive)
        {
            var request = (FtpWebRequest)WebRequest.Create(new Uri(url));
            request.Method = method;

            request.Credentials = new NetworkCredential(_userName, _password);
            request.EnableSsl = _useSsl;
            if (_useSsl)
            {
                var byteArray = Convert.FromBase64String(_certificateAsBase64);

                try
                {
                    var x509Certificate = new X509Certificate(byteArray);
                    request.ClientCertificates = new X509CertificateCollection(new[] { x509Certificate });
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"This is not a valid certificate, file name: {_certificateFileName}", e);
                }
            }
                
            request.UsePassive = true;
            request.UseBinary = true;
            request.KeepAlive = keepAlive;

            return request;
        }

        public void TestConnection()
        {
            if (_useSsl && string.IsNullOrWhiteSpace(_certificateAsBase64))
                throw new ArgumentException("Certificate must be provided when using ftp with SSL!");

            ExtractUrlAndDirectories(out var url, out _);
            var request = CreateFtpWebRequest(url, WebRequestMethods.Ftp.ListDirectory, keepAlive: false);

            try
            {
                var response = request.GetResponse();
                response.Close();
            }
            catch (WebException e)
            {
                var response = (FtpWebResponse)e.Response;
                if (response.StatusCode == FtpStatusCode.ActionNotTakenFileUnavailable)
                {
                    // folder doesn't exist
                    return;
                }

                throw;
            }
            catch (AuthenticationException e)
            {
                throw new AuthenticationException("Make sure that you provided the correct certificate " +
                                                  "and that the url matches the one that is defined in it", e);
            }
        }
    }
}

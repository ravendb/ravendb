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
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RavenFtpClient
    {
        private readonly FtpSettings _ftpSettings;
        private readonly Progress _progress;
        private readonly CancellationToken _cancellationToken;
        private readonly bool _useSsl;
        private const int DefaultBufferSize = 81920;
        private const int DefaultFtpPort = 21;

        public RavenFtpClient(FtpSettings ftpSettings, Progress progress = null, CancellationToken? cancellationToken = null)
        {
            _ftpSettings = ftpSettings;
            _progress = progress;
            _cancellationToken = cancellationToken ?? CancellationToken.None;

            if (_ftpSettings.Url.StartsWith("ftp://", StringComparison.OrdinalIgnoreCase) == false &&
                _ftpSettings.Url.StartsWith("ftps://", StringComparison.OrdinalIgnoreCase) == false)
                _ftpSettings.Url = "ftp://" + _ftpSettings.Url;

            if (_ftpSettings.Url.StartsWith("ftps", StringComparison.OrdinalIgnoreCase))
            {
                _useSsl = true;
                _ftpSettings.Url = _ftpSettings.Url.Replace("ftps://", "ftp://", StringComparison.OrdinalIgnoreCase);
            }

            if (_ftpSettings.Url.EndsWith("/") == false)
                _ftpSettings.Url += "/";

            Debug.Assert(_ftpSettings.Url.StartsWith("ftp://", StringComparison.OrdinalIgnoreCase));
        }

        public void UploadFile(string folderName, string fileName, Stream stream)
        {
            TestConnection();

            _progress?.UploadProgress.SetTotal(stream.Length);
            _progress?.UploadProgress.ChangeState(UploadState.PendingUpload);

            var url = CreateNestedFoldersIfNeeded(folderName);
            url += $"/{fileName}";

            var request = CreateFtpWebRequest(url, WebRequestMethods.Ftp.UploadFile, keepAlive: true);
            var readBuffer = new byte[DefaultBufferSize];

            int count;
            var requestStream = request.GetRequestStream();
            while ((count = stream.Read(readBuffer, 0, readBuffer.Length)) != 0)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                requestStream.Write(readBuffer, 0, count);

                _progress?.UploadProgress.ChangeState(UploadState.Uploading);
                _progress?.UploadProgress.UpdateUploaded(count);
                _progress?.OnUploadProgress();
            }

            requestStream.Flush();
            requestStream.Close();

            _progress?.UploadProgress.ChangeState(UploadState.PendingResponse);
            using (request.GetResponse())
            {
                _progress?.UploadProgress.ChangeState(UploadState.Done);
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
            var uri = new Uri(_ftpSettings.Url);

            dirs = uri.AbsolutePath.TrimStart('/').TrimEnd('/').Split("/").ToList();
            var port = _ftpSettings.Port ?? (uri.Port > 0 ? uri.Port : DefaultFtpPort);
            if (port < 1 || port > 65535)
                throw new ArgumentException("Port number range: 1-65535");

            url = $"{uri.Scheme}://{uri.Host}:{port}";
        }

        private FtpWebRequest CreateFtpWebRequest(string url, string method, bool keepAlive)
        {
            var request = (FtpWebRequest)WebRequest.Create(new Uri(url));
            request.Method = method;

            request.Credentials = new NetworkCredential(_ftpSettings.UserName, _ftpSettings.Password);
            request.EnableSsl = _useSsl;
            if (_useSsl)
            {
                var byteArray = Convert.FromBase64String(_ftpSettings.CertificateAsBase64);

                try
                {
                    var x509Certificate = new X509Certificate(byteArray);
                    request.ClientCertificates = new X509CertificateCollection(new[] { x509Certificate });
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"This is not a valid certificate, file name: {_ftpSettings.CertificateFileName}", e);
                }
            }
                
            request.UsePassive = true;
            request.UseBinary = true;
            request.KeepAlive = keepAlive;

            return request;
        }

        public void TestConnection()
        {
            if (_useSsl && string.IsNullOrWhiteSpace(_ftpSettings.CertificateAsBase64))
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

using System;
using System.IO;
using System.Net;
using System.Threading;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Tests.Util;
using Xunit;

namespace Raven.Tests
{
    public class IISClientTest
    {
        const string HostName = "RavenIISTest";
        const int Port = 80;
        const string ApplicationPool = "RavenIISTest";
        protected const string WebDirectory = @".\RavenIISTestWeb\";
        protected const string DbDirectory = @".\RavenIISTestDb\";

        public IISClientTest()
        {
            IISConfig.RemoveByApplicationPool(ApplicationPool);

            IOExtensions.DeleteDirectory(WebDirectory);
            IOExtensions.DeleteDirectory(DbDirectory);
        }

        public IDocumentStore GetDocumentStore()
        {
            if (!Directory.Exists(WebDirectory))
            {
                var fullPath = Path.GetFullPath(WebDirectory);

                IOExtensions.CopyDirectory(GetRavenWebSource(), WebDirectory);

                PSHostsFile.HostsFile.Set(HostName, "127.0.0.1");
                IISConfig.CreateApplicationPool(ApplicationPool);
                IISConfig.CreateSite(HostName, Port, ApplicationPool, fullPath);

                VerifySiteIsAwake();
            }

            return new DocumentStore()
            {
                Url = GetUrl()
            };
        }

        private void VerifySiteIsAwake()
        {
            var retriesLeft = 3;

            do
            {
                try
                {
                    Console.WriteLine();
                    Console.WriteLine("Downloading " + GetUrl());
                    Console.WriteLine();
                    var request = HttpWebRequest.Create(GetUrl()) as HttpWebRequest;

                    using (var response = request.GetResponse() as HttpWebResponse)
                    {
                        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

                        using (var streamReader = new StreamReader(response.GetResponseStream()))
                        {
                            var responseText = streamReader.ReadToEnd();
                            Assert.Contains("<title>Raven Studio", responseText);
                        }
                    }
                }
                catch (Exception)
                {
                    if (--retriesLeft > 0)
                    {
                        Thread.Sleep(1000);
                        continue;
                    }
                    else
                        throw;
                }

                break;
            } while (true);
        }

        private string GetUrl()
        {
            return string.Format("http://{0}:{1}/", HostName, Port);
        }

        string GetRavenWebSource()
        {
            foreach (var path in new[] { @".\..\..\..\Raven.Web", @".\_PublishedWebsites\Raven.Web" })
            {
                var fullPath = Path.GetFullPath(path);
                
                if (Directory.Exists(fullPath) && Directory.Exists(Path.Combine(fullPath, "bin")))
                {
                    return fullPath;
                }
            }

            throw new FileNotFoundException("Could not find source directory for Raven.Web");
        }
    }
}
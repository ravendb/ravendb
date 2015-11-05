// -----------------------------------------------------------------------
//  <copyright file="WebTestFixture.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Raven.Database.Extensions;
using Raven.Tests.Common.Util;

namespace Raven.Tests.Web
{
    public class WebTestFixture : IDisposable
    {
        public static int Port = 25555;

        public string Url
        {
            get
            {
                return iisExpressDriver.Url;
            }
        }

        private readonly IISExpressDriver iisExpressDriver;

        private readonly string path;

        public WebTestFixture()
        {
            if (IsIisExpressInstalled() == false)
                return;

            try
            {
                path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
                var from = Path.GetFullPath("../");

                IOExtensions.CopyDirectory(from, path);

                iisExpressDriver = new IISExpressDriver();
                iisExpressDriver.Start(path, Port);
            }
            catch (Exception)
            {
                IOExtensions.DeleteDirectory(path);

                throw;
            }
        }

        private static bool IsIisExpressInstalled()
        {
            return File.Exists(@"c:\Program Files (x86)\IIS Express\iisexpress.exe") || File.Exists(@"c:\Program Files\IIS Express\iisexpress.exe");
        }

        public void Dispose()
        {
            if (string.IsNullOrEmpty(path) == false)
                IOExtensions.DeleteDirectory(path);

            if (iisExpressDriver != null)
                iisExpressDriver.Dispose();
        }
    }
}

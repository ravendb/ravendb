//-----------------------------------------------------------------------
// <copyright file="UrlExtension.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Http.Abstractions
{
    public static class UrlExtension
    {
        public static string GetRequestUrl(this IHttpContext context)
        {
            string localPath = context.Request.Url.LocalPath;
            if (context.Configuration.VirtualDirectory != "/" &&
                localPath.StartsWith(context.Configuration.VirtualDirectory, StringComparison.InvariantCultureIgnoreCase))
            {
                localPath = localPath.Substring(context.Configuration.VirtualDirectory.Length);
                if (localPath.Length == 0)
                    localPath = "/";
            }
            return localPath;
        }
    }
}
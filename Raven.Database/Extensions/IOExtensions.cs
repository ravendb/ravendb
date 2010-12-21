//-----------------------------------------------------------------------
// <copyright file="IOExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace Raven.Database.Extensions
{
    public static class IOExtensions
    {
        public static void DeleteDirectory(string directory)
        {
            const int retries = 10;
            for (int i = 0; i < retries; i++)
            {
                try
                {
                    if (Directory.Exists(directory) == false)
                        return;

                    try
                    {
                        File.SetAttributes(directory, FileAttributes.Normal);
                    }
                    catch (IOException)
                    {
                    }
                    Directory.Delete(directory, true);
                    return;
                }
                catch (IOException)
                {
                    foreach (var childDir in Directory.GetDirectories(directory))
                    {
                        try
                        {
                            File.SetAttributes(childDir, FileAttributes.Normal);
                        }
                        catch (IOException)
                        {
                        }
                    }
                    if (i == retries-1)// last try also failed
                        throw;
                    Thread.Sleep(100);
                }
            }
        }

        public static string ToFullPath(this string path)
        {
			if (path.StartsWith(@"~\"))
				path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, path.Substring(2));

            return Path.IsPathRooted(path) ? path : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, path);
        }
    }
}

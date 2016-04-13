// -----------------------------------------------------------------------
//  <copyright file="EnsureTestCleanup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Database.Extensions;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Common.Attributes
{
    public class SuccessfulTestLogRemoval : TestResultCallbackAttribute
    {
        public static HashSet<MethodInfo> failedMethods = new HashSet<MethodInfo>(); 

        public override void After(MethodInfo methodUnderTest, MethodResult result)
        {
            // remove logs when test passed and another favour of test doesn't fail previously (used in case of Theory tests). 
            if (PerTestLogger.ShouldEnablePerTestLog() && result is PassedResult && !failedMethods.Contains(methodUnderTest))
            {
                var fileToDelete = Path.Combine(PerTestLogger.TestsDirName, PerTestLogger.GenerateTestFileName(methodUnderTest));
                if (File.Exists(fileToDelete))
                {
                    IOExtensions.DeleteFile(fileToDelete);
                    var parentDir = Directory.GetParent(fileToDelete);
                    if (!parentDir.EnumerateFileSystemInfos().Any())
                    {
                        IOExtensions.DeleteDirectory(parentDir.FullName);
                    }
                }
            }

            if (result is FailedResult)
            {
                failedMethods.Add(methodUnderTest);
            }
        }
    }
}

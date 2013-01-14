//-----------------------------------------------------------------------
// <copyright file="SetupHelper.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using Microsoft.Isam.Esent.Interop;

namespace PixieTests
{
    /// <summary>
    /// Create a directory and an instance pointed at the directory.
    /// </summary>
    public static class SetupHelper
    {
        /// <summary>
        /// Static object used for locking.
        /// </summary>
        private static readonly object lockObject = new object();

        /// <summary>
        /// Number of instances that have been created. Used to create unique names.
        /// </summary>
        private static int instanceNum;

        /// <summary>
        /// Creates a new random directory in the current working directory. This
        /// should be used to ensure that each test runs in its own directory.
        /// </summary>
        /// <returns>The name of the directory.</returns>
        public static string CreateRandomDirectory()
        {
            string myDir = Path.GetRandomFileName() + @"\";
            Directory.CreateDirectory(myDir);
            return myDir;
        }

        /// <summary>
        /// Create a new instance and set its log/system/temp directories to 
        /// the given directory.
        /// </summary>
        /// <param name="myDir">The directory to use.</param>
        /// <returns>A newly created instance (non-initialized).</returns>
        public static JET_INSTANCE CreateNewInstance(string myDir)
        {
            JET_INSTANCE instance;
            Api.JetCreateInstance(out instance, InstanceName());
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.LogFilePath, 0, myDir);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.SystemPath, 0, myDir);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.TempPath, 0, myDir);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.NoInformationEvent, 1, null);
            return instance;
        }

        /// <summary>
        /// Creates a unique name for a new instance.
        /// </summary>
        /// <returns>An index name.</returns>
        private static string InstanceName()
        {
            lock (lockObject)
            {
                instanceNum++;
                return String.Format("Instance_{0}", instanceNum);
            }
        }
    }
}

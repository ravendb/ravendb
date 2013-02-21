//-----------------------------------------------------------------------
// <copyright file="SetupHelper.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Create a directory and an instance pointed at the directory.
    /// </summary>
    internal static class SetupHelper
    {
        /// <summary>
        /// Number of instances that have been created. Used to create unique names.
        /// </summary>
        private static int instanceNum;

        /// <summary>
        /// Gets a dictionary mapping column types to column definitions.
        /// </summary>
        public static Dictionary<string, JET_COLUMNDEF> ColumndefDictionary
        {
            get
            {
                // Some callers modify the dictionary so we return a new one each time.
                return new Dictionary<string, JET_COLUMNDEF>(StringComparer.OrdinalIgnoreCase)
                {
                    // BUG: Older version of ESENT don't support all column types for temp tables so we'll just use binary columns for the new types.
                    { "Boolean", new JET_COLUMNDEF() { coltyp = JET_coltyp.Bit } },
                    { "Byte", new JET_COLUMNDEF() { coltyp = JET_coltyp.UnsignedByte } },
                    { "Int16", new JET_COLUMNDEF() { coltyp = JET_coltyp.Short } },
                    { "UInt16", new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 2 } },
                    { "Int32", new JET_COLUMNDEF() { coltyp = JET_coltyp.Long } },
                    { "UInt32", new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 4 } },
                    { "Int64", new JET_COLUMNDEF() { coltyp = JET_coltyp.Currency } },
                    { "UInt64", new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 8 } },
                    { "Guid", new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 16 } },
                    { "ASCII", new JET_COLUMNDEF() { coltyp = JET_coltyp.LongText, cp = JET_CP.ASCII } },
                    { "Unicode", new JET_COLUMNDEF() { coltyp = JET_coltyp.LongText, cp = JET_CP.Unicode } },
                    { "Float", new JET_COLUMNDEF() { coltyp = JET_coltyp.IEEESingle } },
                    { "Double", new JET_COLUMNDEF() { coltyp = JET_coltyp.IEEEDouble } },
                    { "DateTime", new JET_COLUMNDEF() { coltyp = JET_coltyp.DateTime } },
                    { "Binary", new JET_COLUMNDEF() { coltyp = JET_coltyp.LongBinary } },
                };
            }
        }

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

            // Set paths
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.LogFilePath, 0, myDir);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.SystemPath, 0, myDir);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.TempPath, 0, myDir);

            // Small logfiles, no events and small temp db
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.LogFileSize, 256, null); // 256Kb
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.PageTempDBMin, SystemParameters.PageTempDBSmallest, null);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.NoInformationEvent, 1, null);
            return instance;
        }

        /// <summary>
        /// Turns off logging, disables the temp DB and turns off events.
        /// </summary>
        /// <param name="instance">The instance to configure.</param>
        public static void SetLightweightConfiguration(Instance instance)
        {
            instance.Parameters.Recovery = false;
            instance.Parameters.NoInformationEvent = true;
            instance.Parameters.MaxTemporaryTables = 0;
        }

        /// <summary>
        /// Creates a standard temp table with a column for each type.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="grbit">Temporary table options.</param>
        /// <param name="tableid">Returns the temporary table.</param>
        /// <returns>A dictionary mapping types to columns.</returns>
        public static Dictionary<string, JET_COLUMNID> CreateTempTableWithAllColumns(JET_SESID sesid, TempTableGrbit grbit, out JET_TABLEID tableid)
        {
            var columnDefs = new List<JET_COLUMNDEF>();
            var columnNames = new List<string>();

            columnDefs.Add(new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.TTKey });
            columnNames.Add("key");

            foreach (KeyValuePair<string, JET_COLUMNDEF> def in ColumndefDictionary)
            {
                columnNames.Add(def.Key);
                columnDefs.Add(def.Value);
            }

            JET_COLUMNDEF[] columns = columnDefs.ToArray();

            // Make all the columns tagged so they don't appear by default
            for (int i = 0; i < columns.Length; ++i)
            {
                columns[i].grbit |= ColumndefGrbit.ColumnTagged;
            }

            var columnids = new JET_COLUMNID[columns.Length];
            Api.JetOpenTempTable(sesid, columns, columns.Length, grbit, out tableid, columnids);
            var columnidDict = new Dictionary<string, JET_COLUMNID>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < columnids.Length; i++)
            {
                columnidDict[columnNames[i]] = columnids[i];
            }

            return columnidDict;
        }

        /// <summary>
        /// Verifies no instances are leaked.
        /// </summary>
        public static void CheckProcessForInstanceLeaks()
        {
            int numInstances;
            JET_INSTANCE_INFO[] instances;
            Api.JetGetInstanceInfo(out numInstances, out instances);

            if (numInstances != 0)
            {
                Console.WriteLine("There are {0} instances remaining! They are:", numInstances);
                foreach (var instanceInfo in instances)
                {
                    string databaseName = string.Empty;
                    if (instanceInfo.szDatabaseFileName != null && instanceInfo.szDatabaseFileName.Count > 0)
                    {
                        databaseName = instanceInfo.szDatabaseFileName[0];
                    }

                    Console.WriteLine(
                        "   szInstanceName={0}, szDatabaseName={1}",
                        instanceInfo.szInstanceName,
                        databaseName);
                }
            }

            Assert.AreEqual(0, numInstances);
        }

        /// <summary>
        /// Creates a unique name for a new instance.
        /// </summary>
        /// <returns>An index name.</returns>
        private static string InstanceName()
        {
            return String.Format("Instance_{0}", Interlocked.Increment(ref instanceNum));
        }
    }
}

//-----------------------------------------------------------------------
// <copyright file="DumpMetaData.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Utilities
{
    using System;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Database utilities.
    /// </summary>
    internal partial class Dbutil
    {
        /// <summary>
        /// Dump the meta-data of the table.
        /// </summary>
        /// <param name="args">Arguments for the command.</param>
        private void DumpMetaData(string[] args)
        {
            if (args.Length != 1)
            {
                throw new ArgumentException("specify the database", "args");
            }

            string database = args[0];

            using (var instance = new Instance("dumpmetadata"))
            {
                instance.Parameters.Recovery = false;
                instance.Init();

                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.ReadOnly);
                    Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.ReadOnly);

                    foreach (string table in Api.GetTableNames(session, dbid))
                    {
                        Console.WriteLine(table);
                        foreach (ColumnInfo column in Api.GetTableColumns(session, dbid, table))
                        {
                            Console.WriteLine("\t{0}", column.Name);
                            Console.WriteLine("\t\tColtyp:     {0}", column.Coltyp);
                            Console.WriteLine("\t\tColumnid:   {0:N0}", column.Columnid);
                            if (JET_coltyp.LongText == column.Coltyp || JET_coltyp.Text == column.Coltyp)
                            {
                                Console.WriteLine("\t\tCode page:  {0}", column.Cp);
                            }

                            Console.WriteLine("\t\tMax length: {0}", column.MaxLength);
                            Console.WriteLine("\t\tGrbit:      {0}", column.Grbit);
                        }

                        foreach (IndexInfo index in Api.GetTableIndexes(session, dbid, table))
                        {
                            Console.WriteLine("\t{0}", index.Name);
                            Console.WriteLine("\t\tGrbit:          {0}", index.Grbit);
                            Console.WriteLine("\t\tCultureInfo:    {0}", index.CultureInfo);
                            Console.WriteLine("\t\tCompareOptions: {0}", index.CompareOptions);

                            foreach (IndexSegment segment in index.IndexSegments)
                            {
                                Console.WriteLine("\t\t\t{0}", segment.ColumnName);
                                Console.WriteLine("\t\t\t\tColtyp:      {0}", segment.Coltyp);
                                Console.WriteLine("\t\t\t\tIsAscending: {0}", segment.IsAscending);
                                Console.WriteLine("\t\t\t\tIsASCII:     {0}", segment.IsASCII);
                            }
                        }
                    }
                }
            }
        }
    }
}

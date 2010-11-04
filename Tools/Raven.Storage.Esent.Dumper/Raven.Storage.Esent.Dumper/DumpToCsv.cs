//-----------------------------------------------------------------------
// <copyright file="DumpToCsv.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System.IO;

namespace Microsoft.Isam.Esent.Utilities
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;

    /// <summary>
    /// Database utilities.
    /// </summary>
    internal partial class Dbutil
    {
        /// <summary>
        /// Quote a string for use in a CSV dump.
        /// </summary>
        /// <param name="s">The string.</param>
        /// <returns>
        /// A quoted version of the string, or the original string
        /// if no quoting is needed.
        /// </returns>
        internal static string QuoteForCsv(string s)
        {
            if (String.IsNullOrEmpty(s))
            {
                return s;
            }

            const string Quote = "\"";
            const string Comma = ",";

            // first, double any existing quotes
            if (s.Contains(Quote))
            {
                s = s.Replace("\"", "\"\"");
            }

            // check to see if we need to add quotes
            // there are five cases where this is needed:
            //  1. Value starts with whitespace
            //  2. Value ends with whitespace
            //  3. Value contains a comma
            //  4. Value contains a quote
            //  5. Value contains a newline
            if (Char.IsWhiteSpace(s[0])
                || Char.IsWhiteSpace(s[s.Length - 1])
                || s.Contains(Comma)
                || s.Contains(Quote)
                || s.Contains(Environment.NewLine))
            {
                s = String.Format("\"{0}\"", s);
            }

            return s;
        }

        /// <summary>
        /// Return the string format of a byte array.
        /// </summary>
        /// <param name="data">The data to format.</param>
        /// <returns>A string representation of the data.</returns>
        internal static string FormatBytes(byte[] data)
        {
            if (null == data)
            {
                return null;
            }
            else
            {
                var sb = new StringBuilder(data.Length * 2);
                foreach (byte b in data)
                {
                    sb.AppendFormat("{0:x2}", b);
                }

                return sb.ToString();
            }
        }

        /// <summary>
        /// Dump a table in CSV format.
        /// </summary>
        /// <param name="args">Arguments for the command.</param>
        public void DumpToCsv(string[] args)
        {
            if (args.Length != 1)
            {
                throw new ArgumentException("specify the database", "args");
            }

            string database = args[0];

            const string Comma = ",";

            using (var instance = new Instance("dumptocsv"))
            {
                instance.Parameters.Recovery = true;
                instance.Init();

				using (var session = new Session(instance))
				{
					JET_DBID dbid;
					Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.ReadOnly);
					Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.ReadOnly);
					var tableNames = Api.GetTableNames(session, dbid);
					if(Directory.Exists("Dump"))
						Directory.Delete("Dump", true);
					Directory.CreateDirectory("Dump");
					foreach (var tableName in tableNames)
					{
						using (var file = File.CreateText(Path.Combine("Dump", tableName + ".csv")))
						{

							var columnFormatters = new List<Func<JET_SESID, JET_TABLEID, string>>();
							var columnNames = new List<string>();

							foreach (ColumnInfo column in Api.GetTableColumns(session, dbid, tableName))
							{
								columnNames.Add(column.Name);

								// create a local variable that will be captured by the lambda functions below
								JET_COLUMNID columnid = column.Columnid;
								switch (column.Coltyp)
								{
									case JET_coltyp.Bit:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsBoolean(s, t, columnid)));
										break;
									case VistaColtyp.LongLong:
									case JET_coltyp.Currency:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsInt64(s, t, columnid)));
										break;
									case JET_coltyp.IEEEDouble:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsDouble(s, t, columnid)));
										break;
									case JET_coltyp.IEEESingle:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsFloat(s, t, columnid)));
										break;
									case JET_coltyp.Long:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsInt32(s, t, columnid)));
										break;
									case JET_coltyp.Text:
									case JET_coltyp.LongText:
										Encoding encoding = (column.Cp == JET_CP.Unicode) ? Encoding.Unicode : Encoding.ASCII;
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsString(s, t, columnid, encoding)));
										break;
									case JET_coltyp.Short:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsInt16(s, t, columnid)));
										break;
									case JET_coltyp.UnsignedByte:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsByte(s, t, columnid)));
										break;
									case JET_coltyp.DateTime:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsDateTime(s, t, columnid)));
										break;
									case VistaColtyp.UnsignedShort:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsUInt16(s, t, columnid)));
										break;
									case VistaColtyp.UnsignedLong:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsUInt32(s, t, columnid)));
										break;
									case VistaColtyp.GUID:
										columnFormatters.Add((s, t) => String.Format("{0}", Api.RetrieveColumnAsGuid(s, t, columnid)));
										break;
									case JET_coltyp.Binary:
									case JET_coltyp.LongBinary:
									default:
										columnFormatters.Add((s, t) => Dbutil.FormatBytes(Api.RetrieveColumn(s, t, columnid)));
										break;
								}
							}

							file.WriteLine(String.Join(Comma, columnNames.ToArray()));

							using (var table = new Table(session, dbid, tableName, OpenTableGrbit.ReadOnly))
							{
								Api.JetSetTableSequential(session, table, SetTableSequentialGrbit.None);

								Api.MoveBeforeFirst(session, table);
								while (Api.TryMoveNext(session, table))
								{
									IEnumerable<string> columnData = from formatter in columnFormatters
									                                 select Dbutil.QuoteForCsv(formatter(session, table));
									file.WriteLine(String.Join(Comma, columnData.ToArray()));
								}

								Api.JetResetTableSequential(session, table, ResetTableSequentialGrbit.None);
							}
						}
					}
				}
            }
        }
    }
}

// -----------------------------------------------------------------------
//  <copyright file="EsentUtil.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Data;
using Microsoft.Isam.Esent.Interop;
using System.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public class EsentUtil
	{
		public static DataTable DumpTable(Session session, Table table)
		{
			var dt = new DataTable(table.Name);
			var cols = Api.GetTableColumns(session, table).ToArray();
			foreach (var col in cols)
			{
				dt.Columns.Add(col.Name);
			}
			Api.MoveBeforeFirst(session, table);
			while (Api.TryMoveNext(session, table))
			{
				var dataRow = dt.NewRow();
				foreach (var col in cols)
				{
					dataRow[col.Name] = GetvalueFromTable(session, table, col);
				}
				dt.Rows.Add(dataRow);
			}

			return dt;
		}

		private static object GetvalueFromTable(Session session, Table table, ColumnInfo col)
		{
			switch (col.Coltyp)
			{
				case JET_coltyp.Long:
					return Api.RetrieveColumnAsInt32(session, table, col.Columnid);
				case JET_coltyp.DateTime:
					return Api.RetrieveColumnAsDateTime(session, table, col.Columnid);
				case JET_coltyp.Binary:
					var bytes = Api.RetrieveColumn(session, table, col.Columnid);
					if (bytes.Length == 16)
						return new Guid(bytes);
					return Convert.ToBase64String(bytes);
				case JET_coltyp.LongText:
				case JET_coltyp.Text:
					return Api.RetrieveColumnAsString(session, table, col.Columnid);
				case JET_coltyp.LongBinary:
					return "long binary val";
				default:
					throw new ArgumentOutOfRangeException("don't know how to handle coltype: " + col.Coltyp);
			}
		}
	}
}
//-----------------------------------------------------------------------
// <copyright file="IndexToDataTable.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Data;
using System.Linq;
using Raven.Database.Plugins;

namespace Raven.Tests.Triggers
{
	public class IndexToDataTable : AbstractIndexUpdateTrigger
	{
		public DataTable DataTable { get; set; }

		public IndexToDataTable()
		{
			DataTable = new DataTable();
			DataTable.Columns.Add("entry", typeof (string));
			DataTable.Columns.Add("Project", typeof(string));
		}


		public override AbstractIndexUpdateTriggerBatcher CreateBatcher(int indexName)
		{
			return new DataTableBatcher(this);
		}

		public class DataTableBatcher : AbstractIndexUpdateTriggerBatcher
		{
			private readonly IndexToDataTable parent;

			public DataTableBatcher(IndexToDataTable parent)
			{
				this.parent = parent;
			}

			public override void OnIndexEntryDeleted(string entryKey)
			{
				lock (parent.DataTable)
				{
					var dataRows = parent.DataTable.Rows.Cast<DataRow>().Where(x => (string) x["entry"] == entryKey).ToArray();
					foreach (var dataRow in dataRows)
					{
						parent.DataTable.Rows.Remove(dataRow);
					}
				}
			}

			public override void OnIndexEntryCreated(string entryKey, Lucene.Net.Documents.Document document)
			{
				lock (parent.DataTable)
				{
					parent.DataTable.Rows.Add(entryKey, document.GetField("Project").StringValue);
				}
			}
		}
	}
}

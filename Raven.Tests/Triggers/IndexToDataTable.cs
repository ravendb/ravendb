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

        public override void OnIndexEntryDeleted(string indexName, string entryKey)
        {
            var dataRows = DataTable.Rows.Cast<DataRow>().Where(x=> (string) x["entry"] == entryKey).ToArray();
            foreach (var dataRow in dataRows)
            {
                DataTable.Rows.Remove(dataRow);
            }
        }

        public override void OnIndexEntryCreated(string indexName, string entryKey, Lucene.Net.Documents.Document document)
        {
            DataTable.Rows.Add(entryKey, document.GetField("Project").StringValue());
        }
    }
}
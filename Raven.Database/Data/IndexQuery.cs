using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Raven.Database.Extensions;

namespace Raven.Database.Data
{
	public class IndexQuery
	{
		public IndexQuery()
		{
			TotalSize = new Reference<int>();
		    PageSize = 128;
		}

		public string Query { get;  set; }

		public Reference<int> TotalSize { get;  private set; }

		public int Start { get;  set; }

		public int PageSize { get;  set; }

		public string[] FieldsToFetch { get; set; }

		public SortedField[] SortedFields { get; set; }

        public DateTime? Cutoff { get; set; }

		public string GetIndexQueryUrl(string operationUrl, string index, string operationName)
		{
			var path = string.Format("{0}/{5}/{1}?query={2}&start={3}&pageSize={4}", operationUrl, index, 
                                     Uri.EscapeDataString(Query),
			                         Start, PageSize, operationName);
			if (FieldsToFetch != null && FieldsToFetch.Length > 0)
			{
				path = FieldsToFetch.Aggregate(
					new StringBuilder(path),
					(sb, field) => sb.Append("&fetch=").Append(Uri.EscapeDataString(field))
					).ToString();
			}
			if (SortedFields != null && SortedFields.Length > 0)
			{
				path = SortedFields.Aggregate(
					new StringBuilder(path),
					(sb, field) => sb.Append("&sort=").Append(field.Descending ? "-" : "").Append(Uri.EscapeDataString(field.Field))
					).ToString();
			}
			if (Cutoff != null)
			{
				path = path + "&cutOff=" + Uri.EscapeDataString(Cutoff.Value.ToString("o", CultureInfo.InvariantCulture));
			}
			return Uri.EscapeUriString(path);
		}
	}
}
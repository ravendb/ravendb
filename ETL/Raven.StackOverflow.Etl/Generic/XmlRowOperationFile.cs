using System;
using System.Collections.Generic;
using System.Xml;
using System.Xml.Linq;
using Rhino.Etl.Core;

namespace Raven.StackOverflow.Etl.Generic
{
	public class XmlRowOperationFile : Rhino.Etl.Core.Operations.AbstractOperation
	{
		private readonly string file;

		public XmlRowOperationFile(string file)
		{
			this.file = file;
		}

		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			if(rows != null)
			{
				foreach (var row in rows)
				{
					yield return row;
				}
			}
			using(var reader = XmlReader.Create(file))
			{
				reader.MoveToContent();
				while(reader.Read())
				{
					if(reader.NodeType != XmlNodeType.Element ||
						reader.LocalName != "row")
						continue;

					var row = new Row();
					if (reader.MoveToFirstAttribute() == false)
						continue;
					do
					{
						object val = reader.Value;
						long longValue;
						DateTime dateTime;
						if (DateTime.TryParse(reader.Value, out dateTime))
							val = dateTime;
						else if (long.TryParse(reader.Value, out longValue))
							val = longValue;
						row[reader.Name] = val;
						
					} while (reader.MoveToNextAttribute());
					yield return row;
				}
			}
		}
	}
}

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

					var element = (XElement)XNode.ReadFrom(reader);

					var row = new Row();
					foreach (var attr in element.Attributes())
					{
						row[attr.Name.LocalName] = attr.Value;
					}
					yield return row;
				}
			}
		}
	}
}
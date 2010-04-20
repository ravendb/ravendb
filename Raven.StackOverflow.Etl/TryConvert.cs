using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;

namespace Raven.StackOverflow.Etl
{
	public class TryConvert<T> : AbstractOperation 
	{
		private readonly TryConverter converter;

		public delegate bool TryConverter(string str, out T val);

		public TryConvert(TryConverter converter)
		{
			this.converter = converter;
		}

		public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
		{
			foreach (var row in rows)
			{
				foreach (var key in row.Cast<DictionaryEntry>().Select(x=>x.Key).ToArray())
				{
					var str = row[key] as string;
					if (str == null)
						continue;

					T val;
					if (converter(str, out val))
						row[key] = val;
				}
				yield return row;
			}
		}
	}
}
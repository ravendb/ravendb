using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Indexing
{
	public class MultiplePutIndexParam
	{
		public IndexDefinition[] Definitions { get; set; }
		public IndexingPriority[] Priorities { get; set; }
		public string[] IndexesNames { get; set; }

	}
}

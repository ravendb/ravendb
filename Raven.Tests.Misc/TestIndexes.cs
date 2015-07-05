using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Indexes;

namespace Raven.Tests.Misc
{
	public class TestIntIndex : AbstractIndexCreationTask<Data>
	{
		public TestIntIndex()
		{
			Map = docs => from doc in docs select new { doc.Int };
		}
	}
	public class TestStrIndex : AbstractIndexCreationTask<Data>
	{
		public TestStrIndex()
		{
			Map = docs => from doc in docs select new { doc.Str };
		}
	}

	public class Data
	{
		public int Int { get; set; }
		public string Str { get; set; }
	}
}

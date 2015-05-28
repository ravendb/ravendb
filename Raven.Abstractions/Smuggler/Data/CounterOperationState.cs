using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Smuggler.Data
{
	public class CounterOperationState
	{
		public long LastWrittenEtag { get; set; }

		public string CounterId { get; set; }
	}
}

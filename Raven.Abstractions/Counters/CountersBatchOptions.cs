using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Counters
{
	public class CountersBatchOptions
	{
		public CountersBatchOptions()
		{
			BatchSizeLimit = 512;
			BatchReadTimeoutInMilliseconds = 5000;
		}
		/// <summary>
		/// Number of counter changes to send in each batch.
		/// <para>Value:</para>
		/// <para>512 by default</para>
		/// </summary>
		/// <value>512 by default</value>
		public int BatchSizeLimit { get; set; }

		/// <summary>
		/// Maximum timeout in milliseconds to wait counter change writes. Exception will be thrown when timeout is elapsed.
		/// <para>Value:</para>
		/// <para>5000 milliseconds by default</para>
		/// </summary>
		/// <value>5000 milliseconds by default</value>
		public int BatchReadTimeoutInMilliseconds { get; set; }
	}
}

using System;
using System.Collections.Generic;

namespace Raven.Database.Counters
{
    public class ReplicationMessage
    {
		public ReplicationMessage()
		{
			Counters = new List<CounterState>();
		}

        public string SendingServerName { get; set; }

        public List<CounterState> Counters { get; set; }

	    public Guid ServerId { get; set; }
    }
}
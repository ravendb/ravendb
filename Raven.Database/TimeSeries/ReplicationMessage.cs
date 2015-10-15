using System;
using System.Collections.Generic;

namespace Raven.Database.TimeSeries
{
    public class ReplicationMessage
    {
        public string SendingServerName { get; set; }

		public Guid ServerId { get; set; }

		public List<ReplicationLogItem> Logs { get; set; }
	}
}
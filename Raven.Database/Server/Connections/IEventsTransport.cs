using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Connections
{
	public interface IEventsTransport : IDisposable
	{
		string Id { get; }
		bool Connected { get; set; }

		event Action Disconnected;
		void SendAsync(object msg);
	}
}

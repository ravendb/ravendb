using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Monitor
{
	public interface IMonitor : IDisposable
	{
		void Start();
		void Stop();
		void OnTimerTick();
	}
}

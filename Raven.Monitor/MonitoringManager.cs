using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Monitor
{
    internal class MonitoringManager : IDisposable
    {
        public static MonitoringManager MonitorManager;
        private MonitorOptions _options;
        private TimeSpan _samplingRate = TimeSpan.FromSeconds(5);
        private List<IMonitor> _monitors = new List<IMonitor>();
        private Timer _timer;
        public MonitoringManager(MonitorOptions options)
        {
            _options = options;
            if(!RavenDocumentStore.Init(options.ServerUrl))
            {
                throw new ArgumentException("Server with url:{0} failed to response.", options.ServerUrl);
            }

        }

        public void Register(IMonitor monitor)
        {
            _monitors.Add(monitor);
        }

        public void Start()
        {
            lock (locker)
            {
                if (started) 
                    return;

                started = true;
                _timer = new Timer(OnMonitorTimerCallback, null, _samplingRate, _samplingRate);
                foreach (var monitor in _monitors)
                {
                    TimerTick += monitor.OnTimerTick;
                    var copy = monitor;
                    Task.Factory.StartNew(() =>
                    {
                        copy.Start();
                    });
                }
            }			
        }

        private void OnMonitorTimerCallback(object state)
        {
            var onTimerTick = TimerTick;
            if (onTimerTick != null) 
                onTimerTick();
        }

        public void Stop()
        {
            if (!started) return;
            _timer.Dispose();
            foreach (var monitor in _monitors)
            {
                TimerTick -= monitor.OnTimerTick;
                monitor.Stop();
            }
            started = false;
        }
        private readonly object locker = new object();
        private bool started = false;
        private event Action TimerTick;

        public void Dispose()
        {
            if(started)
                Stop();
            foreach (var monitor in _monitors)
            {
                monitor.Dispose();
            }
        }
    }
}

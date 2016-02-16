using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace metrics.Reporting
{
    /// <summary>
    ///  A reporter that periodically prints out formatted application metrics to a specified <see cref="TextWriter" />
    /// </summary>
    public abstract class ReporterBase : IReporter
    {
        protected TextWriter Out;
        protected readonly IReportFormatter _formatter;
        protected CancellationTokenSource Token;
        public int Runs { get; set; }

        protected ReporterBase(TextWriter writer, Metrics metrics)
            : this(writer, new HumanReadableReportFormatter(metrics))
        {
            Out = writer;
        }

        protected ReporterBase(TextWriter writer, IReportFormatter formatter)
        {
            Out = writer;
            _formatter = formatter;
        }

        /// <summary>
        /// Starts the reporting task for periodic output
        /// </summary>
        /// <param name="period">The period between successive displays</param>
        /// <param name="unit">The period time unit</param>
        public virtual void Start(long period, TimeUnit unit)
        {
            var seconds = unit.Convert(period, TimeUnit.Seconds);
            var interval = TimeSpan.FromSeconds(seconds);

            Token = new CancellationTokenSource();
            Task.Factory.StartNew(async () =>
            {
                OnStarted();
                while (!Token.IsCancellationRequested)
                {
                    await Task.Delay(interval, Token.Token);
	                if (!Token.IsCancellationRequested)
		                Run();
                }
            }, Token.Token);
        }

        public void Stop()
        {
            Token.Cancel();
            OnStopped();
        }

        public event EventHandler<EventArgs> Started;
        public void OnStarted()
        {
            var handler = Started;
            if (handler != null)
            {
                handler(this, EventArgs.Empty);
            }
        }

        public event EventHandler<EventArgs> Stopped;
        public void OnStopped()
        {
            var handler = Stopped;
            if (handler != null)
            {
                handler(this, EventArgs.Empty);
            }
        }

        public virtual void Run()
        {
            try
            {
                Out.Write(_formatter.GetSample());
                
                Out.Flush();

                Runs++;
            }
            catch (Exception e)
            {
                Out.WriteLine(e.StackTrace);
            }
        }

        public void Dispose()
        {
            if (Token != null)
            {
                Token.Cancel();
            }

            if (Out != null)
            {
                Out.Dispose();
            }
        }
    }
}

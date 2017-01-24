using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Behaviors;

namespace Rachis.Communication
{
    public class IncomingCommunicationThread : IDisposable
    {
        private Action<ITransportBus,CancellationToken> _actualAction;
        private ITransportBus _transport;
        private CancellationTokenSource _cancellationTokenSource;
        private Thread _thread;
        private RaftEngine _engine;
        private const int MaxTimeToWaitForIncomingCommunicationThreadToJoin = 5*1000; //waiting up to 5 seconds for thread to join
        public string SourceId { get; }

        public bool Joined { get; private set; }
        /// <summary>
        /// This event indicates that the communication thread terminated, if the exception is null it means it ended naturally with no error.
        /// </summary>
        public event Action<IncomingCommunicationThread, Exception> CommunicationDone; 
        public IncomingCommunicationThread(string sourceId, string nodeId, ITransportBus transport, RaftEngine engine)
        {
            SourceId = sourceId;
            _engine = engine;
            _transport = transport;          
            _cancellationTokenSource = new CancellationTokenSource();
            
            _thread = new Thread(_threadFunction)
            {
                IsBackground = true,
                Name = $"{sourceId}=>{nodeId}"
            };
            _thread.Start();
        }        

        /// <summary>
        /// this is a wrapper function for the behavior's member function
        /// </summary>
        private void _threadFunction()
        {
            try
            {
                _engine.StateBehavior.HandleNewConnection(_transport, _cancellationTokenSource.Token);
                CommunicationDone?.Invoke(this, null);
            }
            catch (Exception e)
            {
                CommunicationDone?.Invoke(this, e);
            }
            Dispose(); 
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _transport.Dispose();
            Joined = (_thread == Thread.CurrentThread) || _thread.Join(MaxTimeToWaitForIncomingCommunicationThreadToJoin);

        }
    }
}

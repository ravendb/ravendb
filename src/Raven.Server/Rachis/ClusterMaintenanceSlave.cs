using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.TcpHandlers;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public class ClusterMaintenanceSlave : IDisposable
    {
        private readonly TcpConnectionOptions _tcp;
        private readonly CancellationToken _token;

        public ClusterMaintenanceSlave(TcpConnectionOptions tcp, CancellationToken token)
        {
            _tcp = tcp;
            _token = token;

            Task.Factory.StartNew(() =>
            {
                using (tcp.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    while (_token.IsCancellationRequested == false)
                    {


                        Thread.Sleep(500);
                    }
                }
            }, _token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Dispose()
        {            
            _tcp.Dispose();
        }

        //TODO: consider creating finalizer to absolutely make sure we dispose the socket
    }
}

using System;
using System.Net.Sockets;
using System.Threading;
using Raven.Abstractions.Data;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
	    private readonly TcpConnectionHeaderMessage _incomingHeader;
	    private NetworkStream _tcpStream;
	    private readonly DocumentDatabase _database;
	    private readonly Thread _incomingThread;

	    public IncomingReplicationHandler(TcpConnectionHeaderMessage incomingHeader, NetworkStream tcpStream, DocumentDatabase database)
	    {
		    _incomingHeader = incomingHeader;
		    _tcpStream = tcpStream;
		    _database = database;
	    }

	    public void Start()
	    {
		    
	    }

	    public void Dispose()
	    {		
			_tcpStream?.Flush();
			_tcpStream?.Dispose();
		    _tcpStream = null;
	    }
    }
}

using System;
using System.Diagnostics;
using Rachis.Interfaces;
using Rachis.Transport;
using Voron;

namespace Rachis
{
	public class RaftEngineOptions : IDisposable
	{
	    public const int DefaultElectionTimeout = 1200;
	    public const int DefaultHeartbeatTimeout = 300;
	    public const int DefaultMaxLogLengthBeforeCompaction = 32*1024;
	    public static readonly TimeSpan DefaultMaxStepDownDrainTime = TimeSpan.FromSeconds(15);
	    public const int DefaultMaxEntiresPerRequest = 256;

		public RaftEngineOptions(NodeConnectionInfo connection, StorageEnvironmentOptions storageOptions, ITransport transport, IRaftStateMachine stateMachine)
		{
			if (connection == null) throw new ArgumentNullException("connection");
			if (String.IsNullOrWhiteSpace(connection.Name)) throw new ArgumentNullException("connection.Name");
			if (storageOptions == null) throw new ArgumentNullException("storageOptions");
			if (transport == null) throw new ArgumentNullException("transport");
			if (stateMachine == null) throw new ArgumentNullException("stateMachine");

			SelfConnection = connection;
			StorageOptions = storageOptions;
			Transport = transport;
			StateMachine = stateMachine;
		    ElectionTimeout = DefaultElectionTimeout;
		    HeartbeatTimeout = DefaultHeartbeatTimeout;
			Stopwatch = new Stopwatch();
		    MaxLogLengthBeforeCompaction = DefaultMaxLogLengthBeforeCompaction;
		    MaxStepDownDrainTime = DefaultMaxStepDownDrainTime;
			MaxEntriesPerRequest = DefaultMaxEntiresPerRequest;
		}

		public int MaxEntriesPerRequest { get; set; }
		public TimeSpan MaxStepDownDrainTime { get; set; }

		public int MaxLogLengthBeforeCompaction { get; set; }

		public Stopwatch Stopwatch { get; set; }

		public string Name { get { return SelfConnection.Name; } }

		public StorageEnvironmentOptions StorageOptions { get; private set; }

		public ITransport Transport { get; private set; }

		public IRaftStateMachine StateMachine { get; private set; }

		public int ElectionTimeout { get; set; }
		public int HeartbeatTimeout { get; set; }

		public NodeConnectionInfo SelfConnection { get; set; }

		public void Dispose()
		{
			if (Transport != null)
				Transport.Dispose();
		}
	}
}
using System;
using System.Diagnostics;
using Rachis.Interfaces;
using Rachis.Transport;
using Voron;

namespace Rachis
{
	public class RaftEngineOptions
	{
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
			ElectionTimeout = 1200;
			HeartbeatTimeout = 300;
			Stopwatch = new Stopwatch();
			MaxLogLengthBeforeCompaction = 32 * 1024;
			MaxStepDownDrainTime = TimeSpan.FromSeconds(15);
			MaxEntriesPerRequest = 256;
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
	}
}
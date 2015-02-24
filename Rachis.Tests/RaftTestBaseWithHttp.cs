using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rhino.Raft.Interfaces;
using Xunit;

namespace Rhino.Raft.Tests
{
	public class RaftTestBaseWithHttp : RaftTestsBase
	{
		private const short FirstPort = 8100;

		private readonly string _localHost;
		private readonly List<IDisposable> _disposables;


		public RaftTestBaseWithHttp()
		{
			_disposables = new List<IDisposable>();
			var isFiddlerActive =
				Process.GetProcesses().Any(p => p.ProcessName.Equals("fiddler", StringComparison.InvariantCultureIgnoreCase));
			_localHost = isFiddlerActive ? "localhost.fiddler" : "localhost";
		}

		[Fact]
		public void Nodes_should_be_able_to_elect_via_http_transport()
		{
				
		}

		public override void Dispose()
		{
			base.Dispose();
			foreach(var disposable in _disposables)
				disposable.Dispose();
		}
	}
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Counters;

namespace Raven.Database.Smuggler
{
	public class SmugglerCounterApi : ISmugglerApi<CounterConnectionStringOptions, SmugglerCounterOptions, CounterOperationState>
	{
		private CounterStore counterStore;

		public SmugglerCounterApi(CounterStore counterStore)
		{
			if (counterStore == null) throw new ArgumentNullException("counterStore");
			this.counterStore = counterStore;
		}

		public SmugglerCounterOptions Options { get; private set; }

		public Task<CounterOperationState> ExportData(SmugglerExportOptions<CounterConnectionStringOptions> exportOptions)
		{
			throw new NotImplementedException();
		}

		public Task ImportData(SmugglerImportOptions<CounterConnectionStringOptions> importOptions)
		{
			throw new NotImplementedException();
		}

		public Task Between(SmugglerBetweenOptions<CounterConnectionStringOptions> betweenOptions)
		{
			throw new NotImplementedException();
		}
	}
}

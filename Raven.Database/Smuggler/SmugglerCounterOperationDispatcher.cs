using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Smuggler;

namespace Raven.Database.Smuggler
{
    public class SmugglerCounterOperationDispatcher : SmugglerOperationDispatcher<SmugglerCounterOptions>
    {
        private readonly SmugglerCounterApi api;
        public SmugglerCounterOperationDispatcher(SmugglerCounterOptions options) : base(options)
        {
            api = new SmugglerCounterApi
            {
                Options = options,
                ShowProgress = Console.WriteLine
            };
        }

        protected override async Task PerformImportAsync(SmugglerCounterOptions parameters)
        {
            api.Options = parameters;
            await api.ImportData(new SmugglerImportOptions<CounterConnectionStringOptions>
            {
                FromFile = parameters.BackupPath,
                To = parameters.Destination
            }).ConfigureAwait(false);
        }

        protected override async Task PerformExportAsync(SmugglerCounterOptions parameters)
        {
            api.Options = parameters;
            await api.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
            {
                From = parameters.Source,
                ToFile = parameters.BackupPath
            }).ConfigureAwait(false);
        }

        protected override async Task PerformBetweenAsync(SmugglerCounterOptions parameters)
        {
            api.Options = parameters;
            await api.Between(new SmugglerBetweenOptions<CounterConnectionStringOptions>
            {
                From = parameters.Source,
                To = parameters.Destination,
                ReportProgress = api.ShowProgress
            }).ConfigureAwait(false);
        }

        protected override string FileExtension => "ravencsdump";
    }
}

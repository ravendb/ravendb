using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
using System.Linq;
using Caliburn.Micro;
using Raven.ManagementStudio.UI.Silverlight.ViewModels.Interfaces;

namespace Raven.ManagementStudio.UI.Silverlight
{
    public class AppBootstrapper : Bootstrapper<IShell>
    {
        private CompositionContainer _container;

        protected override void Configure()
        {
            var catalog = new AggregateCatalog(
                AssemblySource.Instance.Select(assembly => new AssemblyCatalog(assembly))
                    .Cast<ComposablePartCatalog>());

            _container = CompositionHost.Initialize(catalog);

            var batch = new CompositionBatch();

            batch.AddExportedValue<IWindowManager>(new WindowManager());
            batch.AddExportedValue<IEventAggregator>(new EventAggregator());
            batch.AddExportedValue(_container);
            batch.AddExportedValue(catalog);

            _container.Compose(batch);
        }

        protected override object GetInstance(Type serviceType, string key)
        {
            var contract = string.IsNullOrEmpty(key) ? AttributedModelServices.GetContractName(serviceType) : key;
            var exports = _container.GetExportedValues<object>(contract);

            if (exports.Count() > 0)
            {
                return exports.First();
            }

            throw new InvalidOperationException(string.Format("Could not locate any instances of contract {0}.", contract));
        }

        protected override IEnumerable<object> GetAllInstances(Type serviceType)
        {
            return _container.GetExportedValues<object>(AttributedModelServices.GetContractName(serviceType));
        }

        protected override void BuildUp(object instance)
        {
            _container.SatisfyImportsOnce(instance);
        }
    }
}

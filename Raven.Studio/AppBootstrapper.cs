namespace Raven.Studio
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.ComponentModel.Composition.Hosting;
	using System.ComponentModel.Composition.Primitives;
	using System.Linq;
	using System.Reflection;
	using System.Windows;
	using System.Windows.Controls;
	using Caliburn.Micro;
	using Framework;
	using Shell;
	using Shell.MessageBox;

    public class AppBootstrapper : Bootstrapper<IShell>
	{
		CompositionContainer container;

		protected override void Configure()
		{
			ConfigureConventions();

			var catalog = new AggregateCatalog(
				AssemblySource.Instance.Select(assembly => new AssemblyCatalog(assembly))
					.Cast<ComposablePartCatalog>());

			RegisterTypesByConvention(catalog);
			
			container = CompositionHost.Initialize(catalog);

			var batch = new CompositionBatch();
			batch.AddExportedValue<IWindowManager>(new WindowManager());
			batch.AddExportedValue<IEventAggregator>(new EventAggregator());
			batch.AddExportedValue<ShowMessageBox>(ShowMessageBox);
			batch.AddExportedValue(container);
			batch.AddExportedValue(catalog);

			container.Compose(batch);
		}

		static void RegisterTypesByConvention(AggregateCatalog master)
		{
			var catalog = new ConventionalCatalog();
			
			var commands = from command in Assembly.GetExecutingAssembly().GetExportedTypes()
							where command.Namespace.StartsWith("Raven.Studio.Commands")
							select command;

			commands.Apply(catalog.RegisterWithTypeNameAsKey);

			master.Catalogs.Add(catalog);
		}

		protected override object GetInstance(Type serviceType, string key)
		{
			string contract = string.IsNullOrEmpty(key) ? AttributedModelServices.GetContractName(serviceType) : key;
			IEnumerable<object> exports = container.GetExportedValues<object>(contract);

			if (exports.Count() > 0)
			{
				return exports.First();
			}

			throw new InvalidOperationException(string.Format("Could not locate any instances of contract {0}.", contract));
		}

		protected override IEnumerable<object> GetAllInstances(Type serviceType)
		{
			return container.GetExportedValues<object>(AttributedModelServices.GetContractName(serviceType));
		}

		protected override void BuildUp(object instance)
		{
			container.SatisfyImportsOnce(instance);
		}

		static void ConfigureConventions()
		{
			ConventionManager.AddElementConvention<TransitioningContentControl>(View.ModelProperty, "DataContext", null);
			ConventionManager.AddElementConvention<BusyIndicator>(BusyIndicator.IsBusyProperty, "IsBusy", null);

			var original = ViewLocator.LocateForModelType;
			ViewLocator.LocateForModelType = (t, v, c) => { return LocateForModelType(t, v, c, original); };
		}

		static UIElement LocateForModelType(Type modelType, DependencyObject viewLocation, object context,
		                             Func<Type, DependencyObject, object, UIElement> original)
		{
			string viewTypeName;
			Type viewType;
			
			// case 1: types that are not in the Studio assembly
			if(modelType.Assembly != Assembly.GetExecutingAssembly())
			{
				var name = "Raven.Studio.Data." + modelType.Name;

				if (name.Contains("`"))
					name = name.Substring(0, name.IndexOf("`"));
				
				viewTypeName = name + "View";

				if (context != null)
				{
					viewTypeName = viewTypeName.Remove(viewTypeName.Length - 4, 4);
					viewTypeName = viewTypeName + "." + context;
				}

				viewType = (from assmebly in AssemblySource.Instance
				                from type in assmebly.GetExportedTypes()
				                where type.FullName == viewTypeName
				                select type).FirstOrDefault();

				if (viewType != null)
					return ViewLocator.GetOrCreateViewType(viewType);
			}

			// case 2: simplified convention
			viewTypeName = modelType.FullName + "View";
			viewType = (from assmebly in AssemblySource.Instance
							from type in assmebly.GetExportedTypes()
							where type.FullName == viewTypeName
							select type).FirstOrDefault();

			if (viewType != null)
				return ViewLocator.GetOrCreateViewType(viewType);

			// case 3: apply the default when all else fails
			return original(modelType, viewLocation, context);
		}

		void ShowMessageBox(string message, string title, MessageBoxOptions options = MessageBoxOptions.Ok,
							Action<IMessageBox> callback = null)
		{
            Execute.OnUIThread( ()=>{
			var box = container.GetExportedValue<IMessageBox>();

			box.DisplayName = title;
			box.Message = message;
			box.Options = options;

			if (callback != null)
				box.Deactivated += (s, e) => callback(box);

			container.GetExportedValue<IWindowManager>().ShowDialog(box);
            });
		}
	}
}
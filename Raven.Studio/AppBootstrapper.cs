using System.IO;
using System.Security.Cryptography;

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

            RegisterSettings();
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
			ViewLocator.LocateForModelType = (t, v, c) => { return StudioViewLocator.LocateForModelType(t, v, c, original); };

		    MessageBinder.SpecialValues["$selecteditems"] = context => {
                ListBox listBox;

                if (context.Source is ListBox)
                    listBox = (ListBox)context.Source;
                else {
                    var viewAware = (ViewAware)context.Source.Tag;
                    var parentView = (FrameworkElement)viewAware.GetView();
                    listBox = (ListBox)parentView.FindName("DocumentPageContainer");
                }

		        return listBox.SelectedItems;
		    };
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

        private static void RegisterSettings()
        {
            using (var manifestResourceStream = typeof(AppBootstrapper).Assembly.GetManifestResourceStream("Raven.Studio.Settings.dat"))
            {
                if (manifestResourceStream == null || manifestResourceStream.Length == 0)
                    return;

                using (var reader = new BinaryReader(manifestResourceStream))
                {
                    using (var aes = new AesManaged())
                    {
                        aes.Key = reader.ReadBytes(32);
                        aes.IV = reader.ReadBytes(16);

                        using (var cryptoStream = new CryptoStream(manifestResourceStream, aes.CreateDecryptor(), CryptoStreamMode.Read))
                        using (var cryptoReader = new BinaryReader(cryptoStream))
                        {
                            ActiproSoftware.Products.ActiproLicenseManager.RegisterLicense(
                                cryptoReader.ReadString(), cryptoReader.ReadString());
                        }
                    }
                }
            }
        }
	}
}
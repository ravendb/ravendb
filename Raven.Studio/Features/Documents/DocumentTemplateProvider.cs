namespace Raven.Studio.Features.Documents
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Text;
	using System.Threading.Tasks;
	using System.Windows;
	using System.Windows.Markup;
	using System.Windows.Media;
	using Caliburn.Micro;
	using Framework;
	using Messages;
	using Plugin;

	[Export(typeof (IDocumentTemplateProvider))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class DocumentTemplateProvider : IDocumentTemplateProvider,
	                                        IHandle<CollectionTemplateUpdated>
	{
		readonly TemplateColorProvider colorProvider;
		readonly IServer server;
		readonly Dictionary<string, DataTemplate> templates = new Dictionary<string, DataTemplate>();

		[ImportingConstructor]
		public DocumentTemplateProvider(IServer server, TemplateColorProvider colorProvider, IEventAggregator events)
		{
			this.server = server;
			this.colorProvider = colorProvider;

			events.Subscribe(this);
		}

		public string GetTemplateXamlFor(string key)
		{
			return (key == "projection")
								? GetProjectionTemplateXaml()
								: GetDefaultTemplateXaml(colorProvider.ColorFrom(key));
		}

		public Task<DataTemplate> GetTemplateFor(string key)
		{
			var tcs = new TaskCompletionSource<DataTemplate>();

			if (templates.ContainsKey(key))
			{
				tcs.TrySetResult(templates[key]);
			}
			else
			{
				using (var session = server.OpenSession())
				{
					session.Advanced.AsyncDatabaseCommands
						.GetAttachmentAsync(key + "Template")
						.ContinueWith(get =>
						              	{
						              		if (get.Result == null)
						              		{
						              			ApplyDefaultTemplate(key, tcs);
						              		}
						              		else
						              		{
						              			var encoding = new UTF8Encoding();
						              			var bytes = get.Result.Data;

						              			var xaml = encoding.GetString(bytes, 0, bytes.Length);
						              			var template = Create(xaml);
						              			templates[key] = template;
						              			tcs.TrySetResult(template);
						              		}
						              	});
				}
			}

			return tcs.Task;
		}

		public DataTemplate RetrieveFromCache(string key)
		{
			return templates.ContainsKey(key) ? (templates[key]) : null;
		}

		public void Handle(CollectionTemplateUpdated message)
		{
			var key = message.TemplateKey.Replace("Template",string.Empty);
			var template = Create(message.Xaml ?? GetTemplateXamlFor(key));
			templates[key] = template;
		}

		static string GetDefaultTemplateXaml(Color fill)
		{
			return
				@"<Grid Margin=""0""
				      Width=""120""
				      Height=""60"">
				<Rectangle Fill=""#FFF4F4F5"" />
				<Rectangle Fill=""" +
				fill +
				@"""
						   HorizontalAlignment=""Left""
						   Width=""10"" />
				<Grid Margin=""14,0,0,0"">
					<StackPanel Orientation=""Vertical"">
						<TextBlock Text=""{Binding CollectionType}""
								   TextTrimming=""WordEllipsis""
								   HorizontalAlignment=""Left"" />
						<TextBlock Text=""{Binding DisplayId}""
								   FontSize=""13.333""
								   TextTrimming=""WordEllipsis""
								   HorizontalAlignment=""Left"" />
					</StackPanel>
				</Grid>
			</Grid>";
		}

		static string GetProjectionTemplateXaml()
		{
			return
				@"<Grid Margin=""0""
				      Width=""120""
				      Height=""60"">
				<Rectangle Fill=""#FFF4F4F5"" />
		
				<Grid Margin=""2,0,0,0"">
					<StackPanel Orientation=""Vertical"">
						<TextBlock Text=""{Binding Summary}""
								   TextWrapping=""Wrap""
								   HorizontalAlignment=""Left"" />
					</StackPanel>
				</Grid>
			</Grid>";
		}

		void ApplyDefaultTemplate(string key, TaskCompletionSource<DataTemplate> tcs)
		{
			var templateXaml = GetTemplateXamlFor(key);

			var defaultTemplate = Create(templateXaml);
			templates[key] = defaultTemplate;
			tcs.TrySetResult(defaultTemplate);
		}

		public static DataTemplate Create(string innerXaml)
		{
			DataTemplate template = null;
			Execute.OnUIThread(() =>
			                   	{
			                   		template =
			                   			(DataTemplate) XamlReader.Load(
			                   				@"<DataTemplate xmlns=""http://schemas.microsoft.com/client/2007"" xmlns:x=""http://schemas.microsoft.com/winfx/2006/xaml"">" + innerXaml +
			                   				@"</DataTemplate>"
			                   			               	);
			                   	}
				);
			return template;
		}
	}

	public interface IDocumentTemplateProvider
	{
		string GetTemplateXamlFor(string key);
		Task<DataTemplate> GetTemplateFor(string key);
		DataTemplate RetrieveFromCache(string key);
	}
}
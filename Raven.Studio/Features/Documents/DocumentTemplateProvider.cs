namespace Raven.Studio.Features.Documents
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Text;
	using System.Threading.Tasks;
	using System.Windows;
	using System.Windows.Markup;
	using System.Windows.Media;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Messages;
	using Plugins;
	using Raven.Database.Data;

	[Export(typeof (IDocumentTemplateProvider))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class DocumentTemplateProvider : IDocumentTemplateProvider,
	                                        IHandle<CollectionTemplateUpdated>
	{
		readonly TemplateColorProvider colorProvider;
		readonly IServer server;
		readonly Dictionary<string, DataTemplate> templates = new Dictionary<string, DataTemplate>(StringComparer.InvariantCultureIgnoreCase);
		readonly Dictionary<string, DataTemplate> defaults = new Dictionary<string, DataTemplate>(StringComparer.InvariantCultureIgnoreCase);
		readonly List<string> requested = new List<string>();

		public event EventHandler<EventArgs<DataTemplate>> DataTemplateRetrieved = delegate { };

		[ImportingConstructor]
		public DocumentTemplateProvider(IServer server, TemplateColorProvider colorProvider, IEventAggregator events)
		{
			this.server = server;
			this.colorProvider = colorProvider;

			events.Subscribe(this);
		}

		public string GetDefaultTemplateXamlFor(string key)
		{
			return (key == "projection")
								? GetProjectionTemplateXaml()
								: GetDefaultTemplateXaml(colorProvider.ColorFrom(key));
		}

		public Task<DataTemplate> GetTemplateFor(string key)
		{
			var tcs = new TaskCompletionSource<DataTemplate>();

			// first, check the cache 
			if (templates.ContainsKey(key))
			{
				tcs.TrySetResult(templates[key]);
				return tcs.Task;
			}

			// second, if we've already asked for it, just wait
			lock (requested)
			{
				if (requested.Contains(key))
				{
					DataTemplateRetrieved += (s,e)=> tcs.TrySetResult(e.Value);
					return tcs.Task;
				}
				
				requested.Add(key);
			}

			// if we don't have it and we're not waiting for it, let's request it from the server
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetAttachmentAsync(key + "Template")
					.ContinueWith(get =>
					{
						var template = GenerateTemplateFrom(get.Result,key);
						lock (templates)
						{
							if (!templates.ContainsKey(key)) templates[key] = template;
						}

						// let those waiting know we've go it now 
						DataTemplateRetrieved(this,new EventArgs<DataTemplate>(template));

						tcs.TrySetResult(template);
					});
			}

			return tcs.Task;
		}

		public DataTemplate RetrieveFromCache(string key)
		{
			return templates.ContainsKey(key) ? (templates[key]) : null;
		}

		public DataTemplate GetDefaultTemplate(string key)
		{
			lock (defaults)
			{
				if (!defaults.ContainsKey(key))
				{
					defaults[key] = Create( GetDefaultTemplateXamlFor(key) );
				}
			}

			return defaults[key];
		}

		DataTemplate GenerateTemplateFrom(Attachment attachment, string key)
		{
			string xaml;
			if (attachment == null)
			{
				xaml = GetDefaultTemplateXamlFor(key);
			}
			else
			{
				var encoding = new UTF8Encoding();
				var bytes = attachment.Data;
				xaml = encoding.GetString(bytes, 0, bytes.Length);
			}
			return Create(xaml);
		}

		void IHandle<CollectionTemplateUpdated>.Handle(CollectionTemplateUpdated message)
		{
			var key = message.TemplateKey.Replace("Template",string.Empty);
			var template = Create(message.Xaml ?? GetDefaultTemplateXamlFor(key));
		
			lock (templates)
			{
				templates[key] = template;
			}
		}

		static string GetDefaultTemplateXaml(Color fill)
		{
			return @"
<Grid xmlns=""http://schemas.microsoft.com/winfx/2006/xaml/presentation""
	  xmlns:x=""http://schemas.microsoft.com/winfx/2006/xaml"" 
	  Margin=""0""
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
		string GetDefaultTemplateXamlFor(string key);
		Task<DataTemplate> GetTemplateFor(string key);
		DataTemplate RetrieveFromCache(string key);
		DataTemplate GetDefaultTemplate(string key);
	}
}
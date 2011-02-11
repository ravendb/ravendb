namespace Raven.Studio.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Threading.Tasks;
	using System.Windows;
	using System.Windows.Markup;
	using System.Windows.Media;
	using Caliburn.Micro;
	using Framework;
	using Plugin;

	[Export]
	public class DocumentTemplateProvider
	{
		readonly IServer server;
		readonly TemplateColorProvider colorProvider;
		readonly Dictionary<string,DataTemplate> templates = new Dictionary<string, DataTemplate>();

		[ImportingConstructor]
		public DocumentTemplateProvider(IServer server, TemplateColorProvider colorProvider)
		{
			this.server = server;
			this.colorProvider = colorProvider;
		}

		static string GetDefaultTemplateXaml(Color fill)
		{
			return @"
                <Grid xmlns:cm=""clr-namespace:Caliburn.Micro;assembly=Caliburn.Micro""
				      Margin=""0,0,6,6""
				      Width=""100""
				      Height=""50"">
				<Rectangle Fill=""#FFF4F4F5"" />
				<Rectangle Fill=""" + fill + @"""
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

		public Task<DataTemplate> GetTemplateFor(string key)
		{
			var tcs = new TaskCompletionSource<DataTemplate>();

			if(!string.IsNullOrEmpty(key))
			{
				var fill = colorProvider.ColorFrom(key);
				var defaultTemplate = Create(GetDefaultTemplateXaml(fill));
				tcs.TrySetResult(defaultTemplate);
			}
			else if (templates.ContainsKey(key))
			{
				tcs.TrySetResult(templates[key]);
			} else
			{
				using(var session = server.OpenSession())
				{
					session.Advanced.AsyncDatabaseCommands
						.GetAttachmentAsync(key)
						.ContinueWith(task=>
						              	{
						              		if(task.IsFaulted)
						              		{ 
						              			throw new NotImplementedException("What to do in this case?");
						              		} else
						              		{
						              			var xaml = task.Result.Data.ToString();
						              			var template = Create(xaml);
						              			templates[key] = template;
						              			tcs.TrySetResult(template);
						              		}
						              	});
				}
			}

			return tcs.Task;
		}

		public static DataTemplate Create(string innerXaml)
		{
			DataTemplate template = null;
			Execute.OnUIThread(()=> { template = 
				(DataTemplate)XamlReader.Load(
					@"<DataTemplate xmlns=""http://schemas.microsoft.com/client/2007"">" + innerXaml + @"</DataTemplate>"
				              	);
			}
				);
			return template;
		}
	}
}
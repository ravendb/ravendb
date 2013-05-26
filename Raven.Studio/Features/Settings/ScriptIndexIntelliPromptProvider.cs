using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Studio.Impl;

namespace Raven.Studio.Features.Settings
{
	public class ScriptIndexIntelliPromptProvider : RavenIntelliPromptProvider
	{
		private readonly ScriptedIndexSettingsSectionModel model;

		public ScriptIndexIntelliPromptProvider()
		{
			model = null;
		}
		public ScriptIndexIntelliPromptProvider(ScriptedIndexSettingsSectionModel model)
		{
			this.model = model;
		}

		protected override void AddItemsToSession(CompletionSession session)
		{
			if (model != null)
			{
				foreach (var item in model.IndexItem)
				{
					session.Items.Add(new CompletionItem
					{
						ImageSourceProvider = new CommonImageSourceProvider(CommonImage.PropertyPublic),
						Text = "this." + item,
						AutoCompletePreText = "this." + item
					});
				}
			}

			session.Items.Add(new CompletionItem
			{
				ImageSourceProvider = new CommonImageSourceProvider(CommonImage.MethodPublic),
				Text = "LoadDocument",
				AutoCompletePreText = "LoadDocument",
				DescriptionProvider =
					new HtmlContentProvider("(<em>documentId</em>)<br/>Loads the document with the given id")
			});

			session.Items.Add(new CompletionItem
			{
				ImageSourceProvider = new CommonImageSourceProvider(CommonImage.MethodPublic),
				Text = "PutDocument",
				AutoCompletePreText = "PutDocument",
				DescriptionProvider =
					new HtmlContentProvider("(<em>documentId</em>, <em>doc</em>, <em>meta</em>)<br/>Puts a document with the given id, with doc and metadata objects")
			});
		}
	}
}

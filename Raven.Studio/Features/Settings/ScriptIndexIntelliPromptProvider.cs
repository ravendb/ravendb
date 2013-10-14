using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Json.Linq;
using Raven.Studio.Impl;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Settings
{
	public class ScriptIndexIntelliPromptProvider : RavenIntelliPromptProvider
	{
		private readonly ScriptedIndexSettingsSectionModel model;

		public ScriptIndexIntelliPromptProvider(Observable<RavenJObject> document, bool showDocumentProperied = true) : base(document)
		{
			ShowDocumentProperties = showDocumentProperied;
		}

		protected override void AddItemsToSession(CompletionSession session)
		{
			session.Items.Add(new CompletionItem
			{
				ImageSourceProvider = new CommonImageSourceProvider(CommonImage.ClassPublic),
				Text = "this.",
				AutoCompletePreText = "this."
			});

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

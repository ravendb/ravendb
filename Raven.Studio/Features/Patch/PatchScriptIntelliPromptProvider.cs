using System.Collections.Generic;
using System.Collections.ObjectModel;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Abstractions.Data;
using Raven.Studio.Impl;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Patch
{
    public class PatchScriptIntelliPromptProvider : RavenIntelliPromptProvider
    {
        private readonly IList<PatchValue> patchValues;
 
        public PatchScriptIntelliPromptProvider(IList<PatchValue> patchValues, ObservableCollection<JsonDocument> recentDocuments) : base(recentDocuments)
        {
            this.patchValues = patchValues;
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

			foreach (var patchValue in patchValues)
			{
				session.Items.Add(new CompletionItem
				{
					ImageSourceProvider = new CommonImageSourceProvider(CommonImage.ConstantPublic),
					Text = patchValue.Key,
					AutoCompletePreText = patchValue.Key,
					DescriptionProvider =
						new HtmlContentProvider(string.Format("Script Parameter <em>{0}</em>. Current value: <em>{1}</em>",
															  patchValue.Key, patchValue.Value))
				});
			}
	    }
    }
}
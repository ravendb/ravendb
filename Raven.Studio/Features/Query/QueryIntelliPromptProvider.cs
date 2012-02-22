// -----------------------------------------------------------------------
//  <copyright file="QueryIntelliPromptProvider.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;

namespace Raven.Studio.Features.Query
{
	public class QueryIntelliPromptProvider : ICompletionProvider
	{
		private readonly IList<string> fields;
		private readonly Dictionary<string, List<string>> termsDictionary;

		public QueryIntelliPromptProvider(IList<string> fields, Dictionary<string, List<string>> termsDictionary)
		{
			this.fields = fields;
			this.termsDictionary = termsDictionary;
		}

		public bool RequestSession(IEditorView view, bool canCommitWithoutPopup)
		{
			var currentInterestingToken = GetCurrentInterestingToken(view);

			var session = new CompletionSession
			              {
			              	CanCommitWithoutPopup = canCommitWithoutPopup,
			              	CanFilterUnmatchedItems = true,
			              	MatchOptions = CompletionMatchOptions.UseAcronyms,
			              };

			if (currentInterestingToken != null && currentInterestingToken.EndsWith(":"))
			{
				var field = currentInterestingToken.Substring(0, currentInterestingToken.Length - 1);
				if (termsDictionary.ContainsKey(field) == false)
					return false;
				var terms = termsDictionary[field];
				foreach (var term in terms)
				{
					session.Items.Add(new CompletionItem
					                  {
					                  	Text = term,
					                  	ImageSourceProvider = new CommonImageSourceProvider(CommonImage.PropertyPublic),
					                  	AutoCompletePreText = term + " ",
					                  });
				}
			}
			else
			{
				foreach (var field in fields)
				{
					session.Items.Add(new CompletionItem
					                  {
					                  	Text = field,
					                  	ImageSourceProvider = new CommonImageSourceProvider(CommonImage.PropertyPublic),
					                  	AutoCompletePreText = field + ": ",
					                  });
				}
			}

			if (session.Items.Count == 0) return false;

			session.Open(view);
			return true;
		}

		private static string GetCurrentInterestingToken(IEditorView view)
		{
			var textSnapshotReader = view.GetReader();
			while (true)
			{
				var lastToken = textSnapshotReader.ReadTokenReverse();
				if (lastToken == null) 
					return null;

				var currentInterestingToken = textSnapshotReader.ReadText(lastToken.Length);
				textSnapshotReader.ReadTokenReverse(); // reset the reading of the text
				if(string.IsNullOrWhiteSpace(currentInterestingToken) == false)
					return currentInterestingToken;
			}
		}

		public string Key
		{
			get { throw new System.NotImplementedException(); }
		}

		public IEnumerable<Ordering> Orderings
		{
			get { throw new System.NotImplementedException(); }
		}
	}
}
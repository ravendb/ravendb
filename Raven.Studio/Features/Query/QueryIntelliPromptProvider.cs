// -----------------------------------------------------------------------
//  <copyright file="QueryIntelliPromptProvider.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Query
{
	public class QueryIntelliPromptProvider : ICompletionProvider
	{
		private readonly string indexName;
		private readonly IList<string> fields;
		private readonly Dictionary<string, Dictionary<string, List<string>>> fieldTermsDictionary;

		public QueryIntelliPromptProvider(string indexName, IList<string> fields, Dictionary<string, Dictionary<string, List<string>>> fieldTermsDictionary)
		{
			this.indexName = indexName;
			this.fields = fields;
			this.fieldTermsDictionary = fieldTermsDictionary;
		}

		public bool RequestSession(IEditorView view, bool canCommitWithoutPopup)
		{
			var session = new CompletionSession
			              {
			              	CanCommitWithoutPopup = canCommitWithoutPopup,
			              	CanFilterUnmatchedItems = true,
			              	MatchOptions = CompletionMatchOptions.UseAcronyms,
			              };

			var currentToken = GetInterestingToken(view, 0);
			var prevToken = GetInterestingToken(view, 1);
			string termPrefix = null;
			if (currentToken != null && currentToken.EndsWith(":"))
			{
				PopulateTerm(currentToken, session, view, string.Empty);
			}
			else if (prevToken != null && prevToken.EndsWith(":"))
			{
				termPrefix = currentToken;
				PopulateTerm(prevToken, session, view, termPrefix);
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

			session.Selection = new CompletionSelection(session.Items.First(), CompletionSelectionState.Partial);
			session.Open(view);
			return true;
		}

		private void PopulateTerm(string token, CompletionSession session, IEditorView view, string termPrefix)
		{
			if(termPrefix.StartsWith("\""))
			{
				termPrefix = termPrefix.Substring(1);
			}
			if( termPrefix.EndsWith("\""))
			{
				termPrefix = termPrefix.Substring(0,termPrefix.Length - 1);
			}
			var field = token.Substring(0, token.Length - 1);
			if (fieldTermsDictionary.ContainsKey(field) == false)
				return;
			var termsDictionary = fieldTermsDictionary[field];
			List<string> terms;
			if (termsDictionary.ContainsKey(termPrefix))
			{
				terms = termsDictionary[termPrefix];
			}
			else
			{
				terms = new List<string>();
				termsDictionary[termPrefix] = terms;
				QueryIndexAutoComplete.GetTermsForFieldAsync(indexName, field, terms, termPrefix)
					.ContinueOnSuccessInTheUIThread(() =>
					{
						PopulateTerm(token, session, view, termPrefix);	
						session.Selection = new CompletionSelection(session.Items.First(), CompletionSelectionState.Partial);
						session.Open(view);
					});
			}
			foreach (var term in terms)
			{
				var maybeQuotedTerm = term.IndexOfAny(new[] {' ', '\t'}) == -1 ? term : "\"" + term + "\"";
				session.Items.Add(new CompletionItem
				{
					Text = term,
					ImageSourceProvider = new CommonImageSourceProvider(CommonImage.PropertyPublic),
					AutoCompletePostText = maybeQuotedTerm,
				});
			}
		}

		private static string GetInterestingToken(IEditorView view, int index)
		{
			var i = 0;
			var textSnapshotReader = view.GetReader();
			while (true)
			{
				var lastToken = textSnapshotReader.ReadTokenReverse();
				if (lastToken == null) 
					return null;

				var currentInterestingToken = textSnapshotReader.ReadText(lastToken.Length);
				textSnapshotReader.ReadTokenReverse(); // reset the reading of the text
				if(string.IsNullOrWhiteSpace(currentInterestingToken) == false && i++ == index)
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
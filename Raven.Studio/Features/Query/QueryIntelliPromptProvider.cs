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
        private static readonly HashSet<string> preTermOperators = new HashSet<string>() { "[", "(", "{", "TO", "-", "+"};
	    private bool isInFieldsOnlyMode;

		public QueryIntelliPromptProvider(IList<string> fields, string indexName, Dictionary<string, Dictionary<string, List<string>>> fieldTermsDictionary)
		{
		    this.fields = fields;

            if (indexName == null || fieldTermsDictionary == null)
            {
                isInFieldsOnlyMode = true;
            }
            else
            {
                this.indexName = indexName;
                this.fieldTermsDictionary = fieldTermsDictionary;
            }
		}

		public bool RequestSession(IEditorView view, bool canCommitWithoutPopup)
		{
			var session = new CompletionSession
			              {
			              	CanCommitWithoutPopup = canCommitWithoutPopup,
			              	CanFilterUnmatchedItems = true,
			              	MatchOptions = CompletionMatchOptions.UseAcronyms,
			              };

		    var context = GetCompletionContext(view);

            if (context.Field != null)
            {
                if (!isInFieldsOnlyMode)
                {
                    PopulateTerm(context.Field, session, view, context.Prefix);
                }
            }
			else
			{
				PopulateFields(session);
			}

			if (session.Items.Count == 0) return false;

			session.Selection = new CompletionSelection(session.Items.First(), CompletionSelectionState.Partial);
			session.Open(view);

			return true;
		}

	    private void PopulateFields(CompletionSession session)
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

	    private void PopulateTerm(string field, CompletionSession session, IEditorView view, string termPrefix)
		{
			if(termPrefix.StartsWith("\""))
			{
				termPrefix = termPrefix.Substring(1);
			}
			if( termPrefix.EndsWith("\""))
			{
				termPrefix = termPrefix.Substring(0,termPrefix.Length - 1);
			}

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
			        .ContinueOnSuccessInTheUIThread(
			            () =>
			                {
			                    PopulateTerm(field, session, view, termPrefix);
			                    var completionItem = session.Items.FirstOrDefault();
			                    if (completionItem != null)
			                    {
			                        session.Selection = new CompletionSelection(completionItem,CompletionSelectionState.Partial);
                                    session.Open(view);
			                    }
			                });
			}

			foreach (var term in terms)
			{
				var maybeQuotedTerm = term.IndexOfAny(new[] {' ', '\t'}) == -1 ? term : "\"" + term + "\"";
				session.Items.Add(new CompletionItem
				{
					Text = term,
					ImageSourceProvider = new CommonImageSourceProvider(CommonImage.ConstantPublic),
					AutoCompletePreText = maybeQuotedTerm,
				});
			}
		}

        private static CompletionContext GetCompletionContext(IEditorView view)
        {
            var reader = view.GetReader();

            bool hasSkippedWhitespace = false;

            while (true)
            {
                var token = reader.ReadTokenReverse();

                if (token == null)
                {
                    return new CompletionContext();
                }
                
                if (token.Key == "Whitespace")
                {
                    hasSkippedWhitespace = true;
                    continue;
                }

                var tokenText = reader.PeekText(token.Length);

                if (token.Key == "Field")
                {
                    return new CompletionContext() {Field = GetFieldName(tokenText), Prefix = ""};
                }

                if ((token.Key == "Operator" && preTermOperators.Contains(tokenText))
                    || token.Key == "OpenQuotes"
                    || token.Key == "RangeQueryStart")
                {
                    var field = FindPrecedingField(reader);
                    return new CompletionContext() {Field = field, Prefix = ""};
                }

                if (!hasSkippedWhitespace && (token.Key == "Value" || token.Key == "StringText") )
                {
                    var field = FindPrecedingField(reader);
                    return new CompletionContext() { Field = field, Prefix = tokenText };
                }

                return new CompletionContext();
            }
        }

	    private static string FindPrecedingField(ITextSnapshotReader reader)
	    {
	        while (true)
	        {
                var token = reader.ReadTokenReverse();

                if (token == null)
                {
                    return null;
                }
                else if (token.Key == "Field")
                {
                    return GetFieldName(reader.PeekText(token.Length));
                }
	        }
	    }

	    private static string GetFieldName(string fieldTokenText)
	    {
	        return fieldTokenText.TrimEnd(':').Trim();
	    }

	    public string Key
		{
			get { throw new System.NotImplementedException(); }
		}

		public IEnumerable<Ordering> Orderings
		{
			get { throw new System.NotImplementedException(); }
		}

        private class CompletionContext
        {
            public string Field;
            public string Prefix;
        }
	}

    
}

using System.Collections.Generic;
using System.Linq;
using System.Text;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Impl
{
	public abstract class RavenIntelliPromptProvider : ICompletionProvider
	{
		public string Key { get; private set; }
		public IEnumerable<Ordering> Orderings { get; private set; }
		protected Observable<RavenJObject> DocumentToSample;
		protected IList<JsonDocument> RecentDocuments;
		protected bool ShowDocumentProperties = true;

		protected RavenIntelliPromptProvider(Observable<RavenJObject> documentToSample)
		{
			DocumentToSample = documentToSample;
		}

		protected RavenIntelliPromptProvider(IList<JsonDocument> recentDocuments)
		{
			RecentDocuments = recentDocuments;
		}

		public bool RequestSession(IEditorView view, bool canCommitWithoutPopup)
		{
			var session = new CompletionSession
			{
				CanCommitWithoutPopup = canCommitWithoutPopup,
				CanFilterUnmatchedItems = true,
				MatchOptions = CompletionMatchOptions.UseAcronyms,
			};

			var completionContext = GetCompletionContext(view);

			if (completionContext.IsDocumentProperty && ShowDocumentProperties)
			{
				var properties = GetProperties(completionContext.CompletedPropertyPath);
				foreach (var property in properties)
				{
					session.Items.Add(new CompletionItem
					{
						ImageSourceProvider = new CommonImageSourceProvider(CommonImage.PropertyPublic),
						Text = property,
						AutoCompletePreText = property,
						DescriptionProvider =
							new HtmlContentProvider(string.Format("Document Property <em>{0}</em>", property))
					});
				}
			}
			else if (!completionContext.IsObjectMember)
			{
				AddItemsToSession(session);
			}

			if (session.Items.Count > 0)
			{
				session.Open(view);
				return true;
			}

			return false;
		}

		protected abstract void AddItemsToSession(CompletionSession session);

		protected virtual CompletionContext GetCompletionContext(IEditorView view)
		{
			var reader = view.GetReader();

			var token = reader.ReadTokenReverse();
			var completionContext = new CompletionContext();

			if (token == null)
				return completionContext;

			var tokenText = reader.PeekText(token.Length);
			if (token.Key == "Identifier" || (token.Key == "Punctuation" && tokenText == "."))
			{
				var memberExpression = DetermineFullMemberExpression(tokenText, reader);

				if (memberExpression.Contains("."))
					completionContext.IsObjectMember = true;

				if (memberExpression.StartsWith("this."))
				{
					completionContext.IsDocumentProperty = true;
					var completedPath = memberExpression.Substring("this.".Length);
					var lastDot = completedPath.LastIndexOf('.');
					completedPath = lastDot >= 0 ? completedPath.Substring(0, lastDot) : "";
					completionContext.CompletedPropertyPath = completedPath;
				}
			}

			return completionContext;
		}

		private string DetermineFullMemberExpression(string tokenText, ITextSnapshotReader reader)
		{
			var sb = new StringBuilder(tokenText);
			var token = reader.ReadTokenReverse();
			while (token != null)
			{
				var text = reader.PeekText(token.Length);

				if (token.Key == "Identifier" || (token.Key == "Punctuation" && text == ".") || (token.Key == "Keyword" && text == "this"))
				{
					sb.Insert(0, text);
				}
				else if (token.Key == "CloseSquareBrace")
				{
					var indexExpression = ReadArrayIndexExpression(reader);
					if (indexExpression == null)
					{
						// we're not going to be able to make sense
						// of the rest of the expression, so bail out.
						break;
					}

					sb.Insert(0, indexExpression);
				}
				else if (token.Key == "Whitespace")
				{
					// skip it
				}
				else
				{
					break;
				}

				token = reader.ReadTokenReverse();
			}

			return sb.ToString();
		}

		private string ReadArrayIndexExpression(ITextSnapshotReader reader)
		{
			// we're looking for an expression of the form [123] or [myVariable]
			// if we don't find one, return false.
			string indexValue = null;

			var token = reader.ReadTokenReverse();
			while (token != null)
			{
				var text = reader.PeekText(token.Length);

				if (token.Key == "Identifier" && indexValue == null)
				{
					// substitute 0 for the name of the variable to give us 
					// the best chance of matching something when we look up the path in a document
					indexValue = "0";
				}
				else if ((token.Key == "IntegerNumber") && indexValue == null)
				{
					indexValue = text;
				}
				else if (token.Key == "Whitespace")
				{
					// skip it
				}
				else if (token.Key == "OpenSquareBrace")
				{
					if (indexValue == null)
					{
						// we didn't find a properly formed (and simple) index expression
						// before hitting the square brace
						return null;
					}

					break;
				}

				token = reader.ReadTokenReverse();
			}

			if (indexValue == null)
				return null;

			return "[" + indexValue + "]";
		}

		private IEnumerable<string> GetProperties(string completedPropertyPath = "")
		{
			if (DocumentToSample != null)
				return GetPropertiesAtEndOfPath(DocumentToSample.Value);
			
			if (RecentDocuments != null && RecentDocuments.Count > 0)
			{
				var parsedPath = new RavenJPath(completedPropertyPath);

				var matchingProperties = RecentDocuments.SelectMany(doc => GetPropertiesAtEndOfPath(doc, parsedPath)).Distinct();

				return matchingProperties;
			}

			return new string[0];
		}

		private IEnumerable<string> GetPropertiesAtEndOfPath(RavenJObject document)
		{
			return document != null ? document.Keys : new string[0];
		}

		private IEnumerable<string> GetPropertiesAtEndOfPath(JsonDocument document, RavenJPath path)
		{
			var currentObject = document.DataAsJson.SelectToken(path);

			if (currentObject is RavenJObject)
				return (currentObject as RavenJObject).Keys;

			return new string[0];
		}



		protected class CompletionContext
		{
			public bool IsObjectMember;
			public bool IsDocumentProperty;
			public string CompletedPropertyPath = "";
		}
	}
}

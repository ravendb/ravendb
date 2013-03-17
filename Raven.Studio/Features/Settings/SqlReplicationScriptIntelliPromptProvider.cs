using System.Collections.Generic;
using System.Text;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
	public class SqlReplicationScriptIntelliPromptProvider : ICompletionProvider
	{
		private readonly Observable<RavenJObject> documentToSample;
		private readonly SqlReplicationSettingsSectionModel sqlReplicationSettingsSectionModel;

		public SqlReplicationScriptIntelliPromptProvider(Observable<RavenJObject> documentToSample, SqlReplicationSettingsSectionModel sqlReplicationSettingsSectionModel)
		{
			this.documentToSample = documentToSample;
			this.sqlReplicationSettingsSectionModel = sqlReplicationSettingsSectionModel;
		}

		public string Key { get; private set; }
		public IEnumerable<Ordering> Orderings { get; private set; }


		public bool RequestSession(IEditorView view, bool canCommitWithoutPopup)
		{
			var session = new CompletionSession();

			var completionContext = GetCompletionContext(view);

			if (completionContext.IsDocumentProperty)
			{
				var properties = GetProperties();
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

				foreach (var sqlReplicationTable in sqlReplicationSettingsSectionModel.SelectedReplication.Value.SqlReplicationTables)
				{
					session.Items.Add(new CompletionItem
					{
						ImageSourceProvider = new CommonImageSourceProvider(CommonImage.MethodPublic),
						Text = "replicateTo" + sqlReplicationTable.TableName,
						AutoCompletePreText = "replicateTo" + sqlReplicationTable.TableName,
						DescriptionProvider =
							new HtmlContentProvider("Will update/insert the specified object to the table " + sqlReplicationTable.TableName +
							                        ", using the specified pkName<br/>replicateTo" + sqlReplicationTable.TableName +
							                        "(columnsObj)")
					});
				}
				session.Items.Add(new CompletionItem
				{
					ImageSourceProvider = new CommonImageSourceProvider(CommonImage.MethodPublic),
					Text = "replicateTo",
					AutoCompletePreText = "replicateTo",
					DescriptionProvider =
						 new HtmlContentProvider("Will update/insert the specified object (with the object properties matching the table columns) to the specified table, using the specified pkName<br/>replicateTo(table, columnsObj)")
				});

				session.Items.Add(new CompletionItem
				{
					ImageSourceProvider = new CommonImageSourceProvider(CommonImage.FieldPublic),
					Text = "documentId",
					AutoCompletePreText = "documentId",
					DescriptionProvider =
						 new HtmlContentProvider("The document id for the current document")
				});
			}

			if (session.Items.Count > 0)
			{
				session.Open(view);
				return true;
			}

			return false;
		}

		private IEnumerable<string> GetProperties()
		{
			var matchingProperties = GetPropertiesAtEndOfPath(documentToSample.Value);

			return matchingProperties;
		}

		private IEnumerable<string> GetPropertiesAtEndOfPath(RavenJObject document)
		{
			if (document != null)
			{
				return document.Keys;
			}

			return new string[0];
		}

		private CompletionContext GetCompletionContext(IEditorView view)
		{
			var reader = view.GetReader();

			var token = reader.ReadTokenReverse();
			var completionContext = new CompletionContext();

			if (token == null)
			{
				return completionContext;
			}

			var tokenText = reader.PeekText(token.Length);
			if (token.Key == "Identifier" || (token.Key == "Punctuation" && tokenText == "."))
			{
				var memberExpression = DetermineFullMemberExpression(tokenText, reader);

				if (memberExpression.Contains("."))
				{
					completionContext.IsObjectMember = true;
				}

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
			{
				return null;
			}

			return "[" + indexValue + "]";
		}

		private class CompletionContext
		{
			public bool IsObjectMember;

			public bool IsDocumentProperty;

			public string CompletedPropertyPath = "";
		}
	}
}
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Json.Linq;
using Raven.Studio.Impl;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Settings
{
	public class SqlReplicationScriptIntelliPromptProvider : RavenIntelliPromptProvider
	{
		private readonly SqlReplicationSettingsSectionModel sqlReplicationSettingsSectionModel;

		public SqlReplicationScriptIntelliPromptProvider(Observable<RavenJObject> documentToSample, SqlReplicationSettingsSectionModel sqlReplicationSettingsSectionModel) : base(documentToSample)
		{
			this.sqlReplicationSettingsSectionModel = sqlReplicationSettingsSectionModel;
		}

		protected override void AddItemsToSession(CompletionSession session)
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
	}
}
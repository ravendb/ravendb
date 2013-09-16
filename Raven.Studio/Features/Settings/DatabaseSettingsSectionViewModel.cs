using System.IO;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Studio.Features.JsonEditor;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
    public class DatabaseSettingsSectionViewModel : SettingsSectionModel
    {
        private static JsonSyntaxLanguageExtended JsonLanguage;
        private IEditorDocument databaseEditor;
        private JsonSerializer serializer;

        static DatabaseSettingsSectionViewModel()
        {
            JsonLanguage = new JsonSyntaxLanguageExtended();
        }

        public IEditorDocument DatabaseEditor
        {
            get { return databaseEditor; }
            set { databaseEditor = value; }
        }
        public string CurrentDatabase { get; private set; }

        public DatabaseSettingsSectionViewModel()
        {
            serializer = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer();
            serializer.Formatting = Formatting.Indented;

            SectionName = "Database Settings";
        }

        public override void LoadFor(DatabaseDocument document)
        {
            DatabaseEditor = new EditorDocument
            {
                Text = Serialize(document),
                Language = JsonLanguage,
                IsReadOnly = true
            };

            CurrentDatabase = document.Id;
            
            OnPropertyChanged(() => DatabaseEditor);
        }

	    public override void MarkAsSaved()
	    {
		    HasUnsavedChanges = false;
	    }

	    private string Serialize(DatabaseDocument document)
        {
            using (var streamWriter = new StringWriter())
            {
                serializer.Serialize(streamWriter, document);
                return streamWriter.ToString();
            }
        }
    }
}
using System;
using System.IO;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Studio.Features.JsonEditor;

namespace Raven.Studio.Models
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

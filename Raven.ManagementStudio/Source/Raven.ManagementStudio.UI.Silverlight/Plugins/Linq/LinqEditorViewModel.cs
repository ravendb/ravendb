using System.Linq;
using Caliburn.Micro;
using Raven.ManagementStudio.Plugin;
using System.Collections.Generic;
using Raven.Database;
using Raven.ManagementStudio.UI.Silverlight.Plugins.Common;
using Raven.ManagementStudio.UI.Silverlight.Models;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Linq
{
    public class LinqEditorViewModel : Screen, IRavenScreen
    {
        public LinqEditorViewModel(IDatabase database)
        {
            Database = database;
        }

        public IDatabase Database { get; private set; }

        public IRavenScreen ParentRavenScreen
        {
            get { return null; }
        }

        public SectionType Section
        {
            get { return SectionType.Linq; }
        }

        private IList<DocumentViewModel> _results;

        public IList<DocumentViewModel> Results
        {
            get { return _results; }
            set
            {
                _results = value;
                NotifyOfPropertyChange(() => Results);
            }
        }

        public void Execute()
        {
            if (!string.IsNullOrWhiteSpace(Query))
            {
                Database.IndexSession.LinearQuery(Query, 0, 25,
                                                  o =>
                                                      {
                                                          Results =
                                                              o.Data.Select(x => new DocumentViewModel(new Document(x), Database, this)).ToArray();
                                                      });
            }
        }

        private string _query;
        public string Query
        {
            get { return _query; }
            set
            {
                _query = value;
                NotifyOfPropertyChange(() => Query);
            }
        }
    }
}

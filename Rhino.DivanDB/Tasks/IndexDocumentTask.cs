using System;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Extensions;
using Rhino.DivanDB.Indexing;
using Rhino.DivanDB.Json;

namespace Rhino.DivanDB.Tasks
{
    public class IndexDocumentTask : Task
    {
        public string Key { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentTask - Key: {0}", Key);
        }

        public override void Execute(WorkContext context)
        {
            context.TransactionaStorage.Batch(actions =>
                                             {
                                                 var doc = actions.DocumentByKey(Key);
                                                 if (doc == null)
                                                 {
                                                     actions.Commit();
                                                     return;
                                                 }

                                                 var json = new JsonDynamicObject(doc.ToJson());

                                                 foreach (var viewName in context.ViewStorage.ViewNames)
                                                 {
                                                     var viewFunc = context.ViewStorage.GetViewFunc(viewName);
                                                     if (viewFunc != null)
                                                         context.IndexStorage.Index(viewName, viewFunc, new[] { json, });
                                                 }

                                                 actions.Commit();
                                             });
        }
    }
}
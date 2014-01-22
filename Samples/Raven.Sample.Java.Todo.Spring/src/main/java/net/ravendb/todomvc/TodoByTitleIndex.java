package net.ravendb.todomvc;

import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.client.indexes.AbstractIndexCreationTask;

public class TodoByTitleIndex extends AbstractIndexCreationTask {

    public TodoByTitleIndex() {
        map = "from t in docs.todos select new { t.Title, t.CreationDate } ";
        QTodo t = QTodo.todo;
        index(t.title, FieldIndexing.ANALYZED);
    }

}

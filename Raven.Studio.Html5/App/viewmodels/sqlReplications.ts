import sqlReplication = require("models/sqlReplication");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");

class sqlReplications extends viewModelBase {

    replications = ko.observableArray<sqlReplication>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate() {
        this.replications([sqlReplication.empty(), sqlReplication.empty(), sqlReplication.empty()]);
    }

    attached() {
        $(".script-label").popover({
            html: true,
            trigger: 'hover',
            content: 'Replication scripts use JScript.',
        });
    }

    saveChanges() {
        
    }

    addNewSqlReplication() {
        this.replications.push(sqlReplication.empty());
    }

    removeSqlReplication(repl: sqlReplication) {
        this.replications.remove(repl);
    }
}

export = sqlReplications; 
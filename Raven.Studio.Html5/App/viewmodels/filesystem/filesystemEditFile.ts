import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import pagedList = require("common/pagedList");
import getFilesystemFilesCommand = require("commands/filesystem/getFilesystemFilesCommand");
import pagedResultSet = require("common/pagedResultSet");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import file = require("models/file");

class filesystemEditFile extends viewModelBase {

    fileName = ko.observable<string>();

    activate(args) {
        super.activate(args);

        if (args.id != null) {
            this.fileName(args.id);
        }

        this.fileName.subscribe(x => this.loadEditorForFile(x));
    }

    loadEditorForFile(fileName: string) {
    }

    navigateToFiles() {
    }

}

export = filesystemEditFile;
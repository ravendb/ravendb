import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import {
    getIndexAutoConvertCommand,
    IndexAutoConvertArgs,
    IndexAutoConvertToJsonResultsDto,
} from "commands/database/index/getIndexAutoConvertCommand";
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import convertedIndexesToStaticStorage = require("common/storage/convertedIndexesToStaticStorage");
import assertUnreachable from "components/utils/assertUnreachable";
import fileDownloader = require("common/fileDownloader");

type ConversionOutputType = Raven.Server.Documents.Handlers.Processors.Indexes.ConversionOutputType;

class convertToStaticDialog extends dialogViewModelBase {
    view = require("views/database/indexes/convertToStaticDialog.html");

    readonly indexName: string;
    readonly databaseName: string;

    constructor(indexName: string, databaseName: string) {
        super();

        this.indexName = indexName;
        this.databaseName = databaseName;

        this.downloadConvertedIndex = this.downloadConvertedIndex.bind(this);
        this.createNewStaticIndex = this.createNewStaticIndex.bind(this);
    }

    attached() {
        // empty by design
    }

    close() {
        dialog.close(this);
    }

    private getIndexDefinitionFromJson(result: string): Raven.Client.Documents.Indexes.IndexDefinition {
        // right now we only support one index
        return (JSON.parse(result) as IndexAutoConvertToJsonResultsDto).Indexes[0];
    }

    downloadConvertedIndex(outputType: ConversionOutputType) {
        const args: IndexAutoConvertArgs = {
            name: this.indexName,
            outputType,
            download: true,
        };

        new getIndexAutoConvertCommand(this.databaseName, args)
            .execute()
            .then((result, _, x) => {
                const xhr = x as unknown as XMLHttpRequest;
                const fileName = xhr.getResponseHeader("Content-Disposition").match(/filename="(.*?)"/)[1];

                switch (outputType) {
                    case "CsharpClass":
                        fileDownloader.downloadAsTxt(result, fileName);
                        break;
                    case "Json":
                        fileDownloader.downloadAsJson(JSON.parse(result), fileName);
                        break;
                    default:
                        assertUnreachable(outputType);
                }
            })
            .always(() => {
                this.close();
            });
    }

    async createNewStaticIndex() {
        const args: IndexAutoConvertArgs = {
            name: this.indexName,
            outputType: "Json",
        };

        try {
            const convertedIndex = await new getIndexAutoConvertCommand(this.databaseName, args).execute();

            const newIndexName = convertedIndexesToStaticStorage.saveIndex(
                this.databaseName,
                this.getIndexDefinitionFromJson(convertedIndex) 
            );

            const targetUrl = appUrl.forEditIndex(newIndexName, this.databaseName);
            router.navigate(targetUrl);
        } finally {
            this.close();
        }
    }
}

export = convertToStaticDialog;

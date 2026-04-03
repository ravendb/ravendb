/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import jsonUtil = require("common/jsonUtil");
import ongoingTaskCdcSinkTableModel = require("models/database/tasks/ongoingTaskCdcSinkTableModel");

class ongoingTaskCdcSinkEditModel extends ongoingTaskEditModel {

    connectionStringName = ko.observable<string>();

    tables = ko.observableArray<ongoingTaskCdcSinkTableModel>([]);

    showEditTableArea: KnockoutComputed<boolean>;

    tableSelectedForEdit = ko.observable<ongoingTaskCdcSinkTableModel>();
    editedTableSandbox = ko.observable<ongoingTaskCdcSinkTableModel>();

    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    get studioTaskType(): StudioTaskType {
        return "CdcSink";
    }

    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        super.initializeObservables();

        this.showEditTableArea = ko.pureComputed(() => !!this.editedTableSandbox());

        const innerDirtyFlag = ko.pureComputed(() => !!this.editedTableSandbox() && this.editedTableSandbox().dirtyFlag().isDirty());
        const tablesCount = ko.pureComputed(() => this.tables().length);
        const hasAnyDirtyTable = ko.pureComputed(() => {
            let anyDirty = false;
            this.tables().forEach(table => {
                if (table.dirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });
            return anyDirty;
        });

        this.dirtyFlag = new ko.DirtyFlag([
                innerDirtyFlag,
                this.taskName,
                this.taskState,
                this.mentorNode,
                this.pinMentorNode,
                this.manualChooseMentor,
                this.connectionStringName,
                tablesCount,
                hasAnyDirtyTable
            ],
            false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidation() {
        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });

        this.tables.extend({
            validation: [
                {
                    validator: () => this.tables().length > 0,
                    message: "At least one table must be configured"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            mentorNode: this.mentorNode,
            tables: this.tables,
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink) {
        super.update(dto);
        const configuration = dto.Configuration;

        if (configuration) {
            this.connectionStringName(configuration.ConnectionStringName);
            this.tables(configuration.Tables.map(x => new ongoingTaskCdcSinkTableModel(x, false)));
            this.manualChooseMentor(!!configuration.MentorNode);
            this.pinMentorNode(configuration.PinToMentorNode);
            this.mentorNode(configuration.MentorNode);
        }
    }

    toDto(): Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration {
        return {
            Name: this.taskName(),
            ConnectionStringName: this.connectionStringName(),
            Disabled: this.taskState() === "Disabled",
            Tables: this.tables().map(x => x.toDto()),
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            TaskId: this.taskId,
            Postgres: null,
            SkipInitialLoad: false,
        };
    }

    deleteTable(table: ongoingTaskCdcSinkTableModel) {
        this.tables.remove(x => table.name() === x.name());

        if (this.tableSelectedForEdit() === table) {
            this.editedTableSandbox(null);
            this.tableSelectedForEdit(null);
        }
    }

    editTable(table: ongoingTaskCdcSinkTableModel) {
        this.tableSelectedForEdit(table);
        this.editedTableSandbox(new ongoingTaskCdcSinkTableModel(table.toDto(), false));
    }

    static empty(): ongoingTaskCdcSinkEditModel {
        return new ongoingTaskCdcSinkEditModel(
            {
                TaskName: "",
                TaskType: "CdcSink",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    TaskId: null,
                    PinToMentorNode: false,
                    MentorNode: null,
                    Disabled: false,
                    Tables: [],
                    ConnectionStringName: null,
                    Name: null,
                    SkipInitialLoad: false,
                },
                ConnectionStringName: null,
                FactoryName: null,
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink);
    }
}

export = ongoingTaskCdcSinkEditModel;

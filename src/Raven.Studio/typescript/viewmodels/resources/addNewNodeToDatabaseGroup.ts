import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import distributeSecretCommand = require("commands/database/secrets/distributeSecretCommand");
import databaseInfo = require("models/resources/info/databaseInfo");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import addNodeToDatabaseGroupCommand = require("commands/database/dbGroup/addNodeToDatabaseGroupCommand");

class addNewNodeToDatabaseGroup extends dialogViewModelBase {

    private isEncrypted: boolean;
    
    nodeTag = ko.observable<string>();
    mentorNode = ko.observable<string>();
    manualChooseMentor = ko.observable<boolean>(false);
    
    key = ko.observable<string>();
    confirmation = ko.observable<boolean>(false);
    databaseInfo: databaseInfo;
    
    encryptionSection = ko.observable<setupEncryptionKey>();
    validationGroup: KnockoutValidationGroup;
    
    nodesCanBeAdded: KnockoutComputed<string[]>;
    possibleMentors: KnockoutComputed<string[]>;

    spinners = {
        addNode: ko.observable<boolean>(false)
    };

    constructor(databaseInfo: databaseInfo, isEncrypted: boolean) {
        super();
        
        this.databaseInfo = databaseInfo;
        this.isEncrypted = isEncrypted;
        if (isEncrypted) {
            this.encryptionSection(setupEncryptionKey.forDatabase(this.key, this.confirmation, ko.observable(databaseInfo.name)));
        }
        
        this.bindToCurrentInstance("selectedClusterNode", "selectedMentor");
        
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables() {
        this.nodesCanBeAdded = ko.pureComputed<string[]>(() => {
            const tags = clusterTopologyManager.default.topology().nodes().map(x => x.tag());
            const existingTags = this.databaseInfo.nodes().map(x => x.tag());
            return _.without(tags, ...existingTags);
        });
        
        this.possibleMentors = ko.pureComputed<string[]>(() => {
            return this.databaseInfo.nodes()
                .filter(x => x.type() === "Member")
                .map(x => x.tag());
        });
    }
    
    private initValidation() {
        this.nodeTag.extend({
            required: true
        });
        
        this.mentorNode.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });
        
        if (this.isEncrypted) {
            setupEncryptionKey.setupKeyValidation(this.key);
            setupEncryptionKey.setupConfirmationValidation(this.confirmation);
        }

        this.validationGroup = ko.validatedObservable({
            key: this.key,
            confirmation: this.confirmation,
            nodeTag: this.nodeTag,
            mentorNode: this.mentorNode
        });
    }
    
    activate() {
        if (this.isEncrypted) {
            return this.encryptionSection().generateEncryptionKey();
        }
        return true;
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        $('.add-new-node-to-db-group [data-toggle="tooltip"]').tooltip();

        if (this.isEncrypted) {
            this.encryptionSection().syncQrCode();
            this.setupDisableReasons("#savingKeyData");

            this.key.subscribe(() => {
                this.encryptionSection().syncQrCode();
                // reset confirmation
                this.confirmation(false);
            });
        }
    }
    
    addNode() {
        if (this.isValid(this.validationGroup)) {
            this.spinners.addNode(true);
            
            this.distributeSecretIfNeeded()
                .done(() => {
                    new addNodeToDatabaseGroupCommand(this.databaseInfo.name, this.nodeTag(), this.manualChooseMentor() ? this.mentorNode() : undefined)
                        .execute()
                        .done(() => {
                            this.close();
                        })
                        .always(() => this.spinners.addNode(false));
                })
                .fail(() => this.spinners.addNode(false));
        }
    }
    
    private distributeSecretIfNeeded(): JQueryPromise<void> {
        if (this.isEncrypted) {
            return new distributeSecretCommand(this.databaseInfo.name, this.key(), [this.nodeTag()])
                .execute();
        }
        
        return $.Deferred<void>().resolve();
    }

    selectedClusterNode(node: string) {
        this.nodeTag(node);
    }

    selectedMentor(tag: string) {
        this.mentorNode(tag);
    }
}

export = addNewNodeToDatabaseGroup;

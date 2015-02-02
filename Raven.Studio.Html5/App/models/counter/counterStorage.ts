import server = require('models/counter/counterServerValue');
import resource = require("models/resource");

class counterStorage extends resource{
    static type = 'counterstorage';

    constructor(name: string, isAdminCurrentTenant: boolean = true, private isDisabled: boolean = false) {
        super(name, counterStorage.type, isAdminCurrentTenant);
        this.disabled(isDisabled);
        this.name = name;
    }

    activate() {
        ko.postbox.publish("ActivateCounterStorage", this);
    }
} 

export = counterStorage; 
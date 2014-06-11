import server = require('models/counter/counterServerValue');
import resource = require("models/resource");

class counterStorage extends resource{
    constructor(public name: string, private isDisabled: boolean = false) {
        super(name, 'counterstorage');
        this.disabled(isDisabled);
        this.name = name;
    }

    activate() {
        ko.postbox.publish("ActivateCounterStorage", this);
    }
} 

export = counterStorage; 
import server = require('models/counter/counterServerValue');
import resource = require("models/resource");

class counterStorage extends resource{
    static type = 'counterstorage';

    constructor(public name: string, private isDisabled: boolean = false) {
        super(name, counterStorage.type);
        this.disabled(isDisabled);
        this.name = name;
    }

    activate() {
        ko.postbox.publish("ActivateCounterStorage", this);
    }
} 

export = counterStorage; 
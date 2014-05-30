import server = require('models/counter/counterServerValue');
import resource = require("models/resource");

class counterStorage extends resource{
    constructor(public name: string) {
        super(name, 'counterstorage');
        this.name = name;
    }

    activate() {
        ko.postbox.publish("ActivateCounterStorage", this);
    }
} 

export = counterStorage; 
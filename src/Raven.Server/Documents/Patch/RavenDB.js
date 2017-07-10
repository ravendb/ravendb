var debug_outputs = [];
var number_of_outputs = 0;

function output(msg) {
    if (msg instanceof Object) {
        dump(msg);
    } else {
        number_of_outputs++;
        debug_outputs.push(msg);
    }
    
}

function dump(obj) {
    number_of_outputs++;
    debug_outputs.push(JSON.stringify(obj));
}

function clear_debug_outputs() {
    debug_outputs = [];
    number_of_outputs = 0;
}

String.prototype.trim = String.prototype.trim || function () {
    return this.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
};


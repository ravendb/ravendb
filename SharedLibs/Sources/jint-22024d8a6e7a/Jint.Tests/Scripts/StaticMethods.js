function DateTime() { }

// set static method now()
DateTime.now = function() {
    return new Date();
};

// must run in the same second                
isfalse(undefined == DateTime.now());
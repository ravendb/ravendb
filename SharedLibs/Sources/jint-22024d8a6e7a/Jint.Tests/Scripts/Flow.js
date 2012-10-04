function returnFromSwitch(param) {
    switch(param) {
        case 'a':
            return 'success';
        case 'fall':
        case 'b':
            return 'doom';
        default:
            return 'default';
    }
    return null;
}

function returnFromIf(param) {
    if(param < 10)
        return 'small';
    return 'big';
}

function noreturn() {
    (function() { return 'fake';} )();
}

function noreturn2() {
    'we are in hell';
}

function returnFromFor() {
    for (var i=0; i < 100; i++)
        if ( i == 10)
            return i;
    return 'baaaad';
}

function returnFromTry() {
    try {
        return "try it";
        throw "error";
    } catch (e) {
        return "got it";
    }
    return 'no';
}

function returnFromCatch() {
    try {
        throw "error";
        return "never";
    } catch (e) {
        return "got it";
    }
    return 'no';
}

switch(1) {
    case 2:
    break;
}

switch(2) {
    default:
    break;
}

switch(3) {
}


assert('success',returnFromSwitch('a'));
assert('doom',returnFromSwitch('b'))
assert('doom',returnFromSwitch('fall'));
assert('default',returnFromSwitch('x'));

assert('small',returnFromIf(1));
assert('big',returnFromIf(11));

assert(null,noreturn());
assert(null,noreturn2());

assert(10,returnFromFor());
assert("try it",returnFromTry());
assert("got it",returnFromCatch());
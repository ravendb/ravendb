var x = 1;

try {
    throw "test";
    x = 0;
}
catch (e) {
    assert(1, x);
}

try {
    throw x;
}
catch (e) {

    assert(1, x);
    assert(e, x);
}
finally {
    x = 2;
}

assert(2, x);

// loops should not eat exceptions
var catched = false;
try {
    for (x = 0; x < 10; x++) {
        var z = 1;
        if (x == 3) throw "loop";
    }
}
catch (e) {
    catched = true;
    assert("loop", e);
    assert(3, x);
}

istrue(catched);
assert(3, x);

var error = false;
try {
    var foo = 'bar' in 'baz'; // in throws an exception if right member is not Object
    error = true;
}
catch (e) {
}

assert(false, error);

// no catch statement
var c = false;
try {
    var f = false;
    try {
        throw "error";
        
        // should not reach this code
        assert(true, false);
    }
    finally {
        f = true;
    }
    // should not reach this code
    assert(true, false);
    assert(true, f);
}
catch (e) {
    c = true;
}

assert(true, c);

// no catch statement should not eat inner exceptions

var catch1 = false, catch2 = false;
try {
    try {
        throw "error";
    }
    finally {
        catch1 = true;
    }
}
catch (e) {
    // should be catching the outer throw which was not caught
    catch2 = true;
}

// nested try/catch/finally
assert(catch1, true);
assert(catch2, true);

var catch3 = 0;
var finally1 = 0;
try {
    throw "error";
}
catch (e1) {
    catch3++;
    try {
        throw "error";
    }
    catch (e2) {
        catch3++;
    }
    finally {
        finally1++;
    }
}
finally {
    finally1++;
}

assert(2, catch3);
assert(2, finally1);
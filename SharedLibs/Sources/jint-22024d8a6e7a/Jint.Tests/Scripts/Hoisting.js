// declarations in blocks are available, even if the block is not processed (hoisting)
assert(undefined, foo1);
if(false) {
    var foo1 = 1;
}
assert(undefined, foo1);

assert(undefined, foo2);
for(var foo2 = 1; foo2 < 100; foo2++) {
}
assert(100, foo2);

// var statements in functions should not get through the function
var ok = false;
try {
    // should throw a ReferenceError exception
    assert(undefined, foo3);
    assert(true, false);
}
catch(e) {
    ok = true;
}

assert(true, ok);

function a() {
    var foo3 = 1;
}
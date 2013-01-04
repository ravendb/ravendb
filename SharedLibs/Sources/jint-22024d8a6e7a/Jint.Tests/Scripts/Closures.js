function filter(pred, arr) {
    var len = arr.length;
    var filtered = []; // shorter version of new Array();
    // iterate through every element in the array...
    for (var i = 0; i < len; i = i + 1) {
        var val = arr[i];
        // if the element satisfies the predicate let it through
        if (pred(val)) {
            filtered.push(val);
        }
    }
    return filtered;
}

var someRandomNumbers = [12, 32, 1, 3, 2, 2, 234, 236, 632, 7, 8];

assert(11, someRandomNumbers.length);

function makeGreaterThanPredicate(lowerBound) {
    return function(numberToCheck) {
        return (numberToCheck > lowerBound) ? true : false;
    };
}

var greaterThan10 = makeGreaterThanPredicate(10);
var greaterThan100 = makeGreaterThanPredicate(100);

var a = filter(greaterThan10, someRandomNumbers);
var b = filter(greaterThan100, someRandomNumbers);

assert(5, a.length);
assert(3, b.length);

function foo() {
    var x = "right";
    function bar() {
        assert("right", x);
    }
    callFunc(bar);
}

function callFunc(f) {
    var x = "wrong";
    f();
}

foo();

